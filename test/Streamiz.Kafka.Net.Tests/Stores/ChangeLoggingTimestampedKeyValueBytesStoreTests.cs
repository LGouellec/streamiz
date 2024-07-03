using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Logging;

namespace Streamiz.Kafka.Net.Tests.Stores
{
    public class ChangeLoggingTimestampedKeyValueBytesStoreTests
    {

        private StreamConfig config;
        private ChangeLoggingTimestampedKeyValueBytesStore store;
        private ProcessorContext context;
        private TaskId id;
        private TopicPartition partition;
        private ProcessorStateManager stateManager;
        private Mock<AbstractTask> task;

        private SyncKafkaSupplier kafkaSupplier;
        private IRecordCollector recordCollector;

        private StringSerDes stringSerDes = new();
        private ValueAndTimestampSerDes<string> valueAndTimestampSerDes;

        [SetUp]
        public void Begin()
        {
            valueAndTimestampSerDes = new ValueAndTimestampSerDes<string>(stringSerDes);
            config = new StreamConfig();
            config.ApplicationId = "unit-test-changelogging-tkv";

            id = new TaskId { Id = 0, Partition = 0 };
            partition = new TopicPartition("source", 0);

            kafkaSupplier = new SyncKafkaSupplier();

            var producerConfig = new ProducerConfig();
            producerConfig.ClientId = "producer-1";

            var adminConfig = new AdminClientConfig();
            adminConfig.ClientId = "admin-client";
            
            var producerClient = kafkaSupplier.GetProducer(producerConfig);
            var adminClient = kafkaSupplier.GetAdmin(adminConfig);
            
            recordCollector = new RecordCollector("p-1", config, id, new NoRunnableSensor("s", "s", MetricsRecordingLevel.DEBUG), adminClient);
            recordCollector.Init(ref producerClient);

            var changelogsTopics = new Dictionary<string, string>{
                { "test-store", "test-store-changelog"}
            };

            stateManager = new ProcessorStateManager(
                id,
                new List<TopicPartition> { partition },
                changelogsTopics,
                new MockChangelogRegister(),
                new MockOffsetCheckpointManager());

            task = new Mock<AbstractTask>();
            task.Setup(k => k.Id).Returns(id);

            context = new ProcessorContext(task.Object, config, stateManager, new StreamMetricsRegistry());
            context.UseRecordCollector(recordCollector);

            var inmemorystore = new InMemoryKeyValueStore("test-store");
            store = new ChangeLoggingTimestampedKeyValueBytesStore(inmemorystore);
            store.Init(context, store);
        }

        private Bytes CreateKey(string key)
            => Bytes.Wrap(stringSerDes.Serialize(key, new SerializationContext()));

        private byte[] CreateValue(string value)
            => valueAndTimestampSerDes.Serialize(ValueAndTimestamp<string>.Make(value, DateTime.Now.GetMilliseconds()), new SerializationContext());

        private string FromValue(byte[] valueBytes)
            => valueAndTimestampSerDes.Deserialize(valueBytes, new SerializationContext()).Value;

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
        public void ChangelogPut()
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
            Assert.AreEqual("value", FromValue(list[0].Value));

            consumer.Dispose();
        }

        [Test]
        public void ChangelogDelete()
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

            store.Delete(CreateKey("test"));

            var r = consumer.Consume();

            Assert.AreEqual("test", FromKey(r.Message.Key));
            Assert.AreEqual(null, r.Message.Value);

            Assert.IsNull(store.Get(CreateKey("test")));

            consumer.Dispose();
        }

        [Test]
        public void ChangelogPutAll()
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

            store.PutAll(new List<KeyValuePair<Bytes, byte[]>>{
                KeyValuePair.Create(CreateKey("test"), CreateValue("value")),
                KeyValuePair.Create(CreateKey("test2"), CreateValue("value2")),
            });

            var r = consumer.Consume();

            Assert.AreEqual("test", FromKey(r.Message.Key));
            Assert.AreEqual("value", FromKey(r.Message.Value));

            r = consumer.Consume();

            Assert.AreEqual("test2", FromKey(r.Message.Key));
            Assert.AreEqual("value2", FromKey(r.Message.Value));

            consumer.Dispose();
        }

        [Test]
        public void ChangelogCount()
        {
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

            store.PutAll(new List<KeyValuePair<Bytes, byte[]>>{
                KeyValuePair.Create(CreateKey("test"), CreateValue("value")),
                KeyValuePair.Create(CreateKey("test2"), CreateValue("value2")),
            });
            
            var count = store.ApproximateNumEntries();
            Assert.AreEqual(2, count);
        }

        [Test]
        public void ChangelogPutIfAbsent()
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

            store.PutIfAbsent(CreateKey("test"), CreateValue("value"));
            var b = store.PutIfAbsent(CreateKey("test"), CreateValue("value2"));
            Assert.AreEqual("value", FromValue(b));

            var r = consumer.Consume();

            Assert.AreEqual("test", FromKey(r.Message.Key));
            Assert.AreEqual("value", FromKey(r.Message.Value));

            r = consumer.Consume();

            Assert.IsNull(r);

            consumer.Dispose();
        }

        [Test]
        public void ChangelogRange()
        {
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
            store.Put(CreateKey("test3"), CreateValue("value3"));

            var data = store.Range(CreateKey("test"), CreateKey("test3")).ToList();

            Assert.AreEqual(2, data.Count);
            Assert.AreEqual("test", FromKey(data[0].Key.Get));
            Assert.AreEqual("value", FromValue(data[0].Value));
            Assert.AreEqual("test3", FromKey(data[1].Key.Get));
            Assert.AreEqual("value3", FromValue(data[1].Value));
        }

        [Test]
        public void ChangelogReverseAll()
        {
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
            store.Put(CreateKey("test3"), CreateValue("value3"));

            var data = store.ReverseAll().ToList();

            Assert.AreEqual(2, data.Count);
            Assert.AreEqual("test3", FromKey(data[0].Key.Get));
            Assert.AreEqual("value3", FromValue(data[0].Value));
            Assert.AreEqual("test", FromKey(data[1].Key.Get));
            Assert.AreEqual("value", FromValue(data[1].Value));
        }

        [Test]
        public void ChangelogReverseRange()
        {
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
            store.Put(CreateKey("test3"), CreateValue("value3"));

            var data = store.ReverseRange(CreateKey("test"), CreateKey("test3")).ToList();

            Assert.AreEqual(2, data.Count);
            Assert.AreEqual("test3", FromKey(data[0].Key.Get));
            Assert.AreEqual("value3", FromValue(data[0].Value));
            Assert.AreEqual("test", FromKey(data[1].Key.Get));
            Assert.AreEqual("value", FromValue(data[1].Value));
        }
    }
}
