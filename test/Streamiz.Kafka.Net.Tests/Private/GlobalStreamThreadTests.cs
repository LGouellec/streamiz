using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class GlobalStreamThreadTests
    {
        private Mock<IGlobalStateMaintainer> globalStateMaintainerMock;
        private Mock<IStreamConfig> streamConfigMock;
        private Mock<IConsumer<byte[], byte[]>> globalConsumerMock;

        private GlobalStreamThread globalStreamThread;
        private CancellationTokenSource cancellationTokenSource;

        [SetUp]
        public void SetUp()
        {
            globalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            globalStateMaintainerMock = new Mock<IGlobalStateMaintainer>();
            streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(x => x.PollMs).Returns(1);
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(1);
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(new Dictionary<TopicPartition, long>());

            cancellationTokenSource = new CancellationTokenSource();
            globalStreamThread = new GlobalStreamThread("global", globalConsumerMock.Object, streamConfigMock.Object,
                globalStateMaintainerMock.Object, new StreamMetricsRegistry());
        }

        [TearDown]
        public void TearDown()
        {
            cancellationTokenSource.Cancel();
        }

        [Test]
        public void ShouldConvertExceptionsToStreamsException()
        {
            streamConfigMock.Setup(x => x.PollMs).Throws(new Exception("boom"));

            Assert.Throws<StreamsException>(() => globalStreamThread.Start(cancellationTokenSource.Token));
        }

        [Test]
        public void ShouldBeRunningAfterSuccesfullStart()
        {
            globalStreamThread.Start(cancellationTokenSource.Token);

            // we need to wait for thread to set running state
            Thread.Sleep(100);
            Assert.AreEqual(GlobalThreadState.RUNNING, globalStreamThread.State);
        }

        [Test]
        public void ShouldStopRunningWhenClosedByUser()
        {
            var token = cancellationTokenSource.Token;

            globalStreamThread.Start(token);
            cancellationTokenSource.Cancel();

            // thread should stop after some time
            Thread.Sleep(100);
            Assert.AreEqual(GlobalThreadState.DEAD, globalStreamThread.State);
        }

        [Test]
        public void ShouldStopGlobalConsumer()
        {
            var token = cancellationTokenSource.Token;

            globalStreamThread.Start(token);
            cancellationTokenSource.Cancel();

            // thread should stop after some time
            Thread.Sleep(100);
            globalConsumerMock.Verify(x => x.Close());
        }

        [Test]
        public void ShouldStopGlobalStateMaintainer()
        {
            var token = cancellationTokenSource.Token;

            globalStreamThread.Start(token);
            cancellationTokenSource.Cancel();

            // thread should stop after some time
            Thread.Sleep(100);
            globalStateMaintainerMock.Verify(x => x.Close());
        }

        [Test]
        public void ShouldStopGlobalStateMaintainerEvenIfStoppingConsumerThrows()
        {
            globalConsumerMock.Setup(x => x.Close()).Throws(new Exception());
            var token = cancellationTokenSource.Token;

            globalStreamThread.Start(token);
            cancellationTokenSource.Cancel();

            // thread should stop after some time
            Thread.Sleep(100);
            globalStateMaintainerMock.Verify(x => x.Close());
        }

        [Test]
        public void ShouldAssignTopicsToConsumer()
        {
            var partitionOffsetDictionary = new Dictionary<TopicPartition, long>()
            {
                {new TopicPartition("topic", 0), Offset.Beginning}
            };
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(partitionOffsetDictionary);

            globalStreamThread.Start(cancellationTokenSource.Token);

            var parts = partitionOffsetDictionary.Keys.Select(o => new TopicPartitionOffset(o, Offset.Beginning));
            globalConsumerMock.Verify(x => x.Assign(parts));
        }

        [Test]
        public void ShouldConsumeRecords()
        {
            var result1 = new ConsumeResult<byte[], byte[]>();
            var result2 = new ConsumeResult<byte[], byte[]>();

            globalConsumerMock.SetupSequence(x => x.Consume(It.IsAny<TimeSpan>()))
                .Returns(result1)
                .Returns(result2)
                .Returns((ConsumeResult<byte[], byte[]>) null);

            globalStreamThread.Start(cancellationTokenSource.Token);

            // wait some time so that thread can process data
            Thread.Sleep(100);
            globalStateMaintainerMock.Verify(x => x.Update(result1), Times.Once);
            globalStateMaintainerMock.Verify(x => x.Update(result2), Times.Once);
        }

        [Test]
        public void ShouldNotFlushTooSoon()
        {
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(100);
            globalStreamThread.Start(cancellationTokenSource.Token);

            // this should be true as the thread should wait 100ms to flush
            globalStateMaintainerMock.Verify(x => x.FlushState(), Times.Never);
        }

        [Test]
        public void ShouldFlush()
        {
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(10);
            globalStreamThread.Start(cancellationTokenSource.Token);

            Thread.Sleep(50);
            // we are waiting longer than CommitIntervalMs so thread should already flush at least once
            globalStateMaintainerMock.Verify(x => x.FlushState());
        }
    }
}