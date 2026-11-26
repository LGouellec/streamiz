using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Tests.Helpers;
using ThreadState = Streamiz.Kafka.Net.Processors.ThreadState;

namespace Streamiz.Kafka.Net.Tests.Private
{
    /// <summary>
    /// Tests for https://github.com/LGouellec/streamiz/issues/480 :
    /// a stream thread stuck in PENDING_SHUTDOWN must not leave the client RUNNING forever.
    /// </summary>
    public class StreamThreadShutdownTests
    {
        internal class ShutdownControl
        {
            public readonly ManualResetEventSlim CloseEntered = new(false);
            public readonly ManualResetEventSlim ReleaseClose = new(false);
        }

        // Processor failing on each record and blocking on close until released
        private class FailingAndBlockingProcessor : Streamiz.Kafka.Net.Processors.Public.IProcessor<string, string>
        {
            private readonly ShutdownControl control;

            public FailingAndBlockingProcessor()
            {
            }

            public FailingAndBlockingProcessor(ShutdownControl control)
            {
                this.control = control;
            }

            public void Init(ProcessorContext<string, string> context)
            {
            }

            public void Process(Record<string, string> record)
                => throw new InvalidOperationException("boom");

            public void Close()
            {
                control.CloseEntered.Set();
                control.ReleaseClose.Wait();
            }
        }

        private static Topology BuildTopology(ShutdownControl control)
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic")
                .Process(ProcessorBuilder
                    .New<string, string>()
                    .Processor<FailingAndBlockingProcessor>(control)
                    .Build());
            return builder.Build();
        }

        private static StreamConfig<StringSerDes, StringSerDes> BuildConfig(TimeSpan shutdownTimeout)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-shutdown-" + Guid.NewGuid();
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.InnerExceptionHandler = _ => ExceptionHandlerResponse.FAIL;
            config.StreamThreadShutdownTimeout = shutdownTimeout;
            return config;
        }

        private static void Produce(SyncKafkaSupplier supplier, IStreamConfig config)
        {
            var serdes = new StringSerDes();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            producer.Produce("topic", new Message<byte[], byte[]>
            {
                Key = serdes.Serialize("key1", new SerializationContext()),
                Value = serdes.Serialize("value1", new SerializationContext())
            });
        }

        private static StreamThread CreateThread(Topology topology, IStreamConfig config, SyncKafkaSupplier supplier)
            => StreamThread.Create(
                "thread-0", Guid.NewGuid(), "c0",
                topology.Builder, new StreamMetricsRegistry(), config,
                supplier, supplier.GetAdmin(config.ToAdminConfig("admin")),
                new StatestoreRestoreManager(null),
                0) as StreamThread;

        [Test]
        public void DefaultStreamThreadShutdownTimeout()
        {
            var config = new StreamConfig();
            Assert.AreEqual(TimeSpan.FromSeconds(60), config.StreamThreadShutdownTimeout);
        }

        [Test]
        public async Task ClientGoesToErrorBeforeTheBlockingCloseOfAFailedThread()
        {
            var control = new ShutdownControl();
            // infinite timeout : the close stays blocked until the test release it
            var config = BuildConfig(TimeSpan.Zero);
            var supplier = new SyncKafkaSupplier();
            var states = new List<KafkaStream.State>();

            var stream = new KafkaStream(BuildTopology(control), config, supplier);
            stream.StateChanged += (_, n) =>
            {
                lock (states)
                    states.Add(n);
            };

            try
            {
                await stream.StartAsync();
                AssertExtensions.WaitUntil(() => stream.StreamState == KafkaStream.State.RUNNING,
                    TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(20));

                Produce(supplier, config);

                // the client is in ERROR while the thread is stuck in its close (PENDING_SHUTDOWN)
                AssertExtensions.WaitUntil(() => stream.StreamState == KafkaStream.State.ERROR,
                    TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(20));
                Assert.AreEqual(KafkaStream.State.ERROR, stream.StreamState);
                Assert.IsTrue(control.CloseEntered.Wait(TimeSpan.FromSeconds(10)));
                Assert.IsFalse(control.ReleaseClose.IsSet);
            }
            finally
            {
                control.ReleaseClose.Set();
                stream.Dispose();
            }

            lock (states)
                Assert.AreEqual(KafkaStream.State.NOT_RUNNING, states.Last());
        }

        [Test]
        public void FailedThreadReachesDeadWhenAShutdownStepTimesOut()
        {
            var control = new ShutdownControl();
            var config = BuildConfig(TimeSpan.FromMilliseconds(500));
            var supplier = new SyncKafkaSupplier();
            var source = new CancellationTokenSource();
            var states = new List<ThreadState>();

            var thread = CreateThread(BuildTopology(control), config, supplier);
            thread.StateChanged += (_, _, n) =>
            {
                lock (states)
                    states.Add((ThreadState)n);
            };

            try
            {
                thread.Start(source.Token);
                Produce(supplier, config);

                AssertExtensions.WaitUntil(() => thread.State == ThreadState.DEAD,
                    TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(20));

                Assert.AreEqual(ThreadState.DEAD, thread.State);
                Assert.IsTrue(thread.IsFailed);
                // the close of the task manager is still blocked, but the thread is DEAD
                Assert.IsTrue(control.CloseEntered.IsSet);
                Assert.IsFalse(control.ReleaseClose.IsSet);
                lock (states)
                {
                    Assert.Contains(ThreadState.PENDING_SHUTDOWN, states);
                    Assert.AreEqual(ThreadState.DEAD, states.Last());
                }
            }
            finally
            {
                control.ReleaseClose.Set();
                source.Cancel();
                thread.Dispose();
            }
        }

        [Test]
        public void ThreadStoppedByCancellationIsNotFailed()
        {
            var control = new ShutdownControl();
            control.ReleaseClose.Set();
            var config = BuildConfig(TimeSpan.FromSeconds(10));
            var supplier = new SyncKafkaSupplier();
            var source = new CancellationTokenSource();

            var thread = CreateThread(BuildTopology(control), config, supplier);
            thread.Start(source.Token);
            AssertExtensions.WaitUntil(() => thread.State == ThreadState.RUNNING,
                TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(20));

            source.Cancel();
            thread.Dispose();

            Assert.AreEqual(ThreadState.DEAD, thread.State);
            Assert.IsFalse(thread.IsFailed);
        }

        [Test]
        public void GlobalStreamThreadGoesToDeadWhenPollingFails()
        {
            var globalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            var globalStateMaintainerMock = new Mock<IGlobalStateMaintainer>();
            var streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(x => x.PollMs).Returns(1);
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(1);
            streamConfigMock.Setup(x => x.MaxPollRecords).Returns(10);
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(new Dictionary<TopicPartition, long>());
            globalConsumerMock.Setup(x => x.Consume(It.IsAny<TimeSpan>())).Throws(new Exception("boom"));

            var states = new List<GlobalThreadState>();
            var globalStreamThread = new GlobalStreamThread("global", globalConsumerMock.Object,
                streamConfigMock.Object, globalStateMaintainerMock.Object, new StreamMetricsRegistry());
            globalStreamThread.StateChanged += (_, _, n) =>
            {
                lock (states)
                    states.Add((GlobalThreadState)n);
            };

            globalStreamThread.Start();

            AssertExtensions.WaitUntil(() => globalStreamThread.State == GlobalThreadState.DEAD,
                TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(20));

            Assert.AreEqual(GlobalThreadState.DEAD, globalStreamThread.State);
            lock (states)
            {
                Assert.AreEqual(new List<GlobalThreadState>
                {
                    GlobalThreadState.RUNNING,
                    GlobalThreadState.PENDING_SHUTDOWN,
                    GlobalThreadState.DEAD
                }, states);
            }

            globalConsumerMock.Verify(x => x.Close());
            globalStateMaintainerMock.Verify(x => x.Close());

            // closing the client after the failure must not throw
            globalStreamThread.Dispose();
            Assert.AreEqual(GlobalThreadState.DEAD, globalStreamThread.State);
        }

        #region Restore phase retry

        private static StoreChangelogReader CreateChangelogReader(IConsumer<byte[], byte[]> restoreConsumer)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-restore-retry";
            return new StoreChangelogReader(config, restoreConsumer, "thread-0",
                new StatestoreRestoreManager(null), new StreamMetricsRegistry())
            {
                QueryWatermarkOffsetsRetryBackoff = TimeSpan.FromMilliseconds(1)
            };
        }

        [Test]
        public void QueryWatermarkOffsetsRetriesRetriableErrors()
        {
            var tp = new TopicPartition("changelog", 0);
            var consumerMock = new Mock<IConsumer<byte[], byte[]>>();
            consumerMock.SetupSequence(x => x.QueryWatermarkOffsets(tp, It.IsAny<TimeSpan>()))
                .Throws(new KafkaException(ErrorCode.NotLeaderForPartition))
                .Throws(new KafkaException(ErrorCode.LeaderNotAvailable))
                .Returns(new WatermarkOffsets(0, 10));

            var reader = CreateChangelogReader(consumerMock.Object);
            var offsets = reader.QueryWatermarkOffsetsWithRetry(tp);

            Assert.AreEqual(10, offsets.High.Value);
            consumerMock.Verify(x => x.QueryWatermarkOffsets(tp, It.IsAny<TimeSpan>()), Times.Exactly(3));
        }

        [Test]
        public void QueryWatermarkOffsetsRetryIsBounded()
        {
            var tp = new TopicPartition("changelog", 0);
            var consumerMock = new Mock<IConsumer<byte[], byte[]>>();
            consumerMock.Setup(x => x.QueryWatermarkOffsets(tp, It.IsAny<TimeSpan>()))
                .Throws(new KafkaException(ErrorCode.NotLeaderForPartition));

            var reader = CreateChangelogReader(consumerMock.Object);
            reader.QueryWatermarkOffsetsMaxAttempts = 3;

            var e = Assert.Throws<KafkaException>(() => reader.QueryWatermarkOffsetsWithRetry(tp));
            Assert.AreEqual(ErrorCode.NotLeaderForPartition, e.Error.Code);
            consumerMock.Verify(x => x.QueryWatermarkOffsets(tp, It.IsAny<TimeSpan>()), Times.Exactly(3));
        }

        [Test]
        public void QueryWatermarkOffsetsDoesNotRetryNonRetriableErrors()
        {
            var tp = new TopicPartition("changelog", 0);
            var consumerMock = new Mock<IConsumer<byte[], byte[]>>();
            consumerMock.Setup(x => x.QueryWatermarkOffsets(tp, It.IsAny<TimeSpan>()))
                .Throws(new KafkaException(ErrorCode.TopicAuthorizationFailed));

            var reader = CreateChangelogReader(consumerMock.Object);

            Assert.Throws<KafkaException>(() => reader.QueryWatermarkOffsetsWithRetry(tp));
            consumerMock.Verify(x => x.QueryWatermarkOffsets(tp, It.IsAny<TimeSpan>()), Times.Once);
        }

        #endregion
    }
}
