using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class GlobalStateUpdateTaskTests
    {
        private Mock<GlobalProcessorContext> contextMock;

        private Mock<IGlobalStateManager> stateManagerMock;

        private Mock<IProcessor> processorMock;

        private Mock<ISourceProcessor> sourceProcessorMock;

        private Mock<ISourceProcessor> otherSourceProcessorMock;

        private GlobalStateUpdateTask globalStateUpdateTask;

        [SetUp]
        public void Setup()
        {
            contextMock = new Mock<GlobalProcessorContext>(null, null);
            stateManagerMock = new Mock<IGlobalStateManager>();
            processorMock = new Mock<IProcessor>();
            sourceProcessorMock = new Mock<ISourceProcessor>();
            otherSourceProcessorMock = new Mock<ISourceProcessor>();

            sourceProcessorMock.Setup(x => x.TopicName).Returns("topic1");
            otherSourceProcessorMock.Setup(x => x.TopicName).Returns("topic2");

            var sourceProcessors = new Dictionary<string, IProcessor>() { { "source1", sourceProcessorMock.Object }, { "source2", otherSourceProcessorMock.Object } };
            var processors = new Dictionary<string, IProcessor>() { { "processor", processorMock.Object } };

            var storesToTopics = new Dictionary<string, string>() {
                { "store1", "topic1" },
                { "store2", "topic2" },
            };

            stateManagerMock.Setup(x => x.Initialize()).Returns(new HashSet<string>() { "store1", "store2" });

            var topology = new ProcessorTopology(
                    null,
                    sourceProcessors,
                    null,
                    processors,
                    null,
                    null,
                    storesToTopics,
                    null);

            globalStateUpdateTask = new GlobalStateUpdateTask(stateManagerMock.Object, topology, contextMock.Object); ;
        }

        [Test]
        public void ShouldInitializeStateManager()
        {
            globalStateUpdateTask.Initialize();

            stateManagerMock.Verify(x => x.Initialize(), Times.Once);
        }

        [Test]
        public void ShouldInitializeProcessors()
        {
            globalStateUpdateTask.Initialize();

            processorMock.Verify(x => x.Init(contextMock.Object), Times.Once);
        }

        [Test]
        public void ShouldProcessRecordsForTopic()
        {
            var key = Encoding.ASCII.GetBytes("key");
            var value = Encoding.ASCII.GetBytes("value");
            globalStateUpdateTask.Initialize();
            var record = new ConsumeResult<byte[], byte[]>()
            {
                Topic = sourceProcessorMock.Object.TopicName,
                Message = new Message<byte[], byte[]>()
                {
                    Key = key,
                    Value = value
                }
            };

            globalStateUpdateTask.Update(record);

            sourceProcessorMock.Verify(x => x.Process(record), Times.Once);
            otherSourceProcessorMock.Verify(x => x.Process(record), Times.Never);
        }

        [Test]
        public void ShouldProcessRecordsForOtherTopic()
        {
            var key = Encoding.ASCII.GetBytes("key");
            var value = Encoding.ASCII.GetBytes("value");
            globalStateUpdateTask.Initialize();

            var record = new ConsumeResult<byte[], byte[]>()
            {
                Topic = otherSourceProcessorMock.Object.TopicName,
                Message = new Message<byte[], byte[]>()
                {
                    Key = key,
                    Value = value
                }
            };

            globalStateUpdateTask.Update(record);

            sourceProcessorMock.Verify(x => x.Process(record), Times.Never);
            otherSourceProcessorMock.Verify(x => x.Process(record), Times.Once);
        }

        [Test]
        public void ShouldFlushState()
        {
            globalStateUpdateTask.Initialize();

            globalStateUpdateTask.FlushState();

            stateManagerMock.Verify(x => x.Flush(), Times.Once);
        }
    }
}
