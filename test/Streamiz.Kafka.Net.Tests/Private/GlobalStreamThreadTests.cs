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
        [Test]
        public void ShouldConvertExceptionsToStreamsException()
        {
            var globalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            var globalStateMaintainerMock = new Mock<IGlobalStateMaintainer>();
            var streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(x => x.PollMs).Returns(1);
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(1);
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(new Dictionary<TopicPartition, long>());

            var globalStreamThread = new GlobalStreamThread("global", globalConsumerMock.Object, streamConfigMock.Object,
                globalStateMaintainerMock.Object, new StreamMetricsRegistry());
            
            streamConfigMock.Setup(x => x.PollMs).Throws(new Exception("boom"));

            Assert.Throws<StreamsException>(() => globalStreamThread.Start());
            globalStreamThread.Dispose();
        }

        [Test]
        public void ShouldBeRunningAfterSuccesfullStart()
        {
            var globalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            var globalStateMaintainerMock = new Mock<IGlobalStateMaintainer>();
            var streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(x => x.PollMs).Returns(1);
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(1);
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(new Dictionary<TopicPartition, long>());

            var globalStreamThread = new GlobalStreamThread("global", globalConsumerMock.Object, streamConfigMock.Object,
                globalStateMaintainerMock.Object, new StreamMetricsRegistry());
            
            globalStreamThread.Start();

            // we need to wait for thread to set running state
            Thread.Sleep(100);
            Assert.AreEqual(GlobalThreadState.RUNNING, globalStreamThread.State);

            globalStreamThread.Dispose();
        }

        [Test]
        public void ShouldStopRunningWhenClosedByUser()
        {
            var globalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            var globalStateMaintainerMock = new Mock<IGlobalStateMaintainer>();
            var streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(x => x.PollMs).Returns(1);
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(1);
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(new Dictionary<TopicPartition, long>());

            var globalStreamThread = new GlobalStreamThread("global", globalConsumerMock.Object, streamConfigMock.Object,
                globalStateMaintainerMock.Object, new StreamMetricsRegistry());

            globalStreamThread.Start();
            // thread should stop after some time
            Thread.Sleep(100);
            globalStreamThread.Dispose();
            Assert.AreEqual(GlobalThreadState.DEAD, globalStreamThread.State);
        }

        [Test]
        public void ShouldStopGlobalConsumer()
        {
            var globalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            var globalStateMaintainerMock = new Mock<IGlobalStateMaintainer>();
            var streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(x => x.PollMs).Returns(1);
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(1);
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(new Dictionary<TopicPartition, long>());

            var globalStreamThread = new GlobalStreamThread("global", globalConsumerMock.Object, streamConfigMock.Object,
                globalStateMaintainerMock.Object, new StreamMetricsRegistry());

            globalStreamThread.Start();

            // thread should stop after some time
            Thread.Sleep(100);
            globalStreamThread.Dispose();
            globalConsumerMock.Verify(x => x.Close());
        }

        [Test]
        public void ShouldStopGlobalStateMaintainer()
        {
            var globalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            var globalStateMaintainerMock = new Mock<IGlobalStateMaintainer>();
            var streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(x => x.PollMs).Returns(1);
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(1);
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(new Dictionary<TopicPartition, long>());

            var globalStreamThread = new GlobalStreamThread("global", globalConsumerMock.Object, streamConfigMock.Object,
                globalStateMaintainerMock.Object, new StreamMetricsRegistry());
            
            globalStreamThread.Start();
            
            // thread should stop after some time
            Thread.Sleep(100);
            globalStreamThread.Dispose();
            globalStateMaintainerMock.Verify(x => x.Close());
        }

        [Test]
        public void ShouldStopGlobalStateMaintainerEvenIfStoppingConsumerThrows()
        {
            var globalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            var globalStateMaintainerMock = new Mock<IGlobalStateMaintainer>();
            var streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(x => x.PollMs).Returns(1);
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(1);
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(new Dictionary<TopicPartition, long>());

            var globalStreamThread = new GlobalStreamThread("global", globalConsumerMock.Object, streamConfigMock.Object,
                globalStateMaintainerMock.Object, new StreamMetricsRegistry());
            
            globalConsumerMock.Setup(x => x.Close()).Throws(new Exception());

            globalStreamThread.Start();

            // thread should stop after some time
            Thread.Sleep(100);
            globalStreamThread.Dispose();
            
            globalStateMaintainerMock.Verify(x => x.Close());
        }

        [Test]
        public void ShouldAssignTopicsToConsumer()
        {
            var globalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            var globalStateMaintainerMock = new Mock<IGlobalStateMaintainer>();
            var streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(x => x.PollMs).Returns(1);
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(1);
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(new Dictionary<TopicPartition, long>());

            var globalStreamThread = new GlobalStreamThread("global", globalConsumerMock.Object, streamConfigMock.Object,
                globalStateMaintainerMock.Object, new StreamMetricsRegistry());
            
            var partitionOffsetDictionary = new Dictionary<TopicPartition, long>()
            {
                {new TopicPartition("topic", 0), Offset.Beginning}
            };
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(partitionOffsetDictionary);

            globalStreamThread.Start();

            var parts = partitionOffsetDictionary.Keys.Select(o => new TopicPartitionOffset(o, Offset.Beginning));
            globalConsumerMock.Verify(x => x.Assign(parts));
            globalStreamThread.Dispose();
        }

        [Test]
        public void ShouldConsumeRecords()
        {
            var globalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            var globalStateMaintainerMock = new Mock<IGlobalStateMaintainer>();
            var streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(x => x.PollMs).Returns(1);
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(1);
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(new Dictionary<TopicPartition, long>());

            var globalStreamThread = new GlobalStreamThread("global", globalConsumerMock.Object, streamConfigMock.Object,
                globalStateMaintainerMock.Object, new StreamMetricsRegistry());
            
            var result1 = new ConsumeResult<byte[], byte[]>();
            var result2 = new ConsumeResult<byte[], byte[]>();

            globalConsumerMock.SetupSequence(x => x.Consume(It.IsAny<TimeSpan>()))
                .Returns(result1)
                .Returns(result2)
                .Returns((ConsumeResult<byte[], byte[]>) null);

            globalStreamThread.Start();

            // wait some time so that thread can process data
            Thread.Sleep(100);
            globalStateMaintainerMock.Verify(x => x.Update(result1), Times.Once);
            globalStateMaintainerMock.Verify(x => x.Update(result2), Times.Once);
            globalStreamThread.Dispose();
        }

        [Test]
        public void ShouldNotFlushTooSoon()
        {
            var globalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            var globalStateMaintainerMock = new Mock<IGlobalStateMaintainer>();
            var streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(x => x.PollMs).Returns(1);
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(1);
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(new Dictionary<TopicPartition, long>());

            var globalStreamThread = new GlobalStreamThread("global", globalConsumerMock.Object, streamConfigMock.Object,
                globalStateMaintainerMock.Object, new StreamMetricsRegistry());
            
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(100);
            globalStreamThread.Start();

            // this should be true as the thread should wait 100ms to flush
            globalStateMaintainerMock.Verify(x => x.FlushState(), Times.Never);
            globalStreamThread.Dispose();
        }

        [Test]
        public void ShouldFlush()
        {
            var globalConsumerMock = new Mock<IConsumer<byte[], byte[]>>();
            var globalStateMaintainerMock = new Mock<IGlobalStateMaintainer>();
            var streamConfigMock = new Mock<IStreamConfig>();
            streamConfigMock.Setup(x => x.PollMs).Returns(1);
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(1);
            globalStateMaintainerMock.Setup(x => x.Initialize()).Returns(new Dictionary<TopicPartition, long>());

            var globalStreamThread = new GlobalStreamThread("global", globalConsumerMock.Object, streamConfigMock.Object,
                globalStateMaintainerMock.Object, new StreamMetricsRegistry());
            
            streamConfigMock.Setup(x => x.CommitIntervalMs).Returns(10);
            globalStreamThread.Start();

            Thread.Sleep(50);
            // we are waiting longer than CommitIntervalMs so thread should already flush at least once
            globalStateMaintainerMock.Verify(x => x.FlushState());
            globalStreamThread.Dispose();
        }
    }
}