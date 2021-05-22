using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class ChangeLoggingTimestampedKeyValueBytesStoreTests
    {

        private StreamConfig config = null;
        private ChangeLoggingTimestampedKeyValueBytesStore store = null;
        private ProcessorContext context = null;
        private TaskId id = null;
        private TopicPartition partition = null;
        private ProcessorStateManager stateManager = null;
        private Mock<AbstractTask> task = null;

        private SyncKafkaSupplier kafkaSupplier = null;
        private IRecordCollector recordCollector = null;
        private StringSerDes stringSerDes = new StringSerDes();

        [SetUp]
        public void Begin()
        {
            config = new StreamConfig();
            config.ApplicationId = $"unit-test-changelogging-tkv";

            id = new TaskId { Id = 0, Partition = 0 };
            partition = new TopicPartition("source", 0);

            kafkaSupplier = new SyncKafkaSupplier();

            var producerConfig = new ProducerConfig();
            producerConfig.ClientId = "producer-1";
            var producerClient = kafkaSupplier.GetProducer(producerConfig);

            recordCollector = new RecordCollector("p-1", config, id);
            recordCollector.Init(ref producerClient);

            var changelogsTopics = new Dictionary<string, string>{
                { "test-store", "test-store-changelog"}
            };

            stateManager = new ProcessorStateManager(id, new List<TopicPartition> { partition }, changelogsTopics);
            
            task = new Mock<AbstractTask>();
            task.Setup(k => k.Id).Returns(id);

            context = new ProcessorContext(task.Object, config, stateManager);
            context.UseRecordCollector(recordCollector);

            var inmemorystore = new InMemoryKeyValueStore("test-store");
            store = new ChangeLoggingTimestampedKeyValueBytesStore(inmemorystore);
            store.Init(context, store);
        }

        private Bytes CreateKey(string key)
            => Bytes.Wrap(stringSerDes.Serialize(key, new SerializationContext()));

        private byte[] CreateValue(string value)
            => stringSerDes.Serialize(value, new SerializationContext());

        private string FromValue(byte[] valueBytes)
            => stringSerDes.Deserialize(valueBytes, new SerializationContext());

        private string FromKey(byte[] keyBytes)
            => stringSerDes.Deserialize(keyBytes, new SerializationContext());


        [TearDown]
        public void End()
        {
            if (store != null)
            {
                store.Flush();
                stateManager.Close();
                recordCollector?.Close();
            }
        }

        [Test]
        public void ChangelogPutAndConsumeTopic()
        {
            var consumerConfig = new ConsumerConfig();
            consumerConfig.GroupId = "test-result-store-changelog";
            var consumer = kafkaSupplier.GetConsumer(consumerConfig, null);
            consumer.Subscribe("test-store-changelog");

            var message = new Message<byte[], byte[]>
            {
                Headers = new Headers(),
                Timestamp = new Timestamp(DateTime.Now)
            };

            var consumerResult = new ConsumeResult<byte[], byte[]>
            {
                Message = message,
                Offset = 0,
                Topic = "test-store",
                Partition = 0
            };

            context.SetRecordMetaData(consumerResult);

            store.Put(CreateKey("test"), CreateValue("value"));

            var r = consumer.Consume();

            Assert.AreEqual("test", FromKey(r.Message.Key));
            Assert.AreEqual("value", FromKey(r.Message.Value));

            var list = store.All().ToList();

            Assert.AreEqual(1, list.Count);
            Assert.AreEqual("test", FromKey(list[0].Key.Get));
            Assert.AreEqual("value", FromKey(list[0].Value));

            consumer.Dispose();
        }
    }
}
