﻿using System;
using System.Collections.Generic;
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
    public class ChangeLoggingTimestampedWindowBytesStoreTests
    {

        private StreamConfig config;
        private ChangeLoggingTimestampedWindowBytesStore store;
        private ProcessorContext context;
        private TaskId id;
        private TopicPartition partition;
        private ProcessorStateManager stateManager;
        private Mock<AbstractTask> task;

        private SyncKafkaSupplier kafkaSupplier;
        private IRecordCollector recordCollector;

        private static StringSerDes stringSerDes = new StringSerDes();
        private static ValueAndTimestampSerDes<string> valueAndTsSerDes = new ValueAndTimestampSerDes<string>(stringSerDes);
        private static TimeWindowedSerDes<string> windowSerDes = new TimeWindowedSerDes<string>(stringSerDes, TimeSpan.FromSeconds(1).Milliseconds);

        [SetUp]
        public void Begin()
        {
            config = new StreamConfig();
            config.ApplicationId = "unit-test-changelogging-tw";

            id = new TaskId { Id = 0, Partition = 0 };
            partition = new TopicPartition("source", 0);

            kafkaSupplier = new SyncKafkaSupplier();

            var streamsProducer = new StreamsProducer(config,
                "thread",
                Guid.NewGuid(),
                kafkaSupplier, "log");
            
            recordCollector = new RecordCollector("p-1", config, id, streamsProducer, new NoRunnableSensor("s", "s", MetricsRecordingLevel.DEBUG));
            
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

            var inmemorystore = new InMemoryWindowStore("test-store", TimeSpan.FromDays(1), TimeSpan.FromSeconds(1).Milliseconds, false);
            store = new ChangeLoggingTimestampedWindowBytesStore(inmemorystore, false);
            store.Init(context, store);
        }

        private Bytes CreateKey(string key)
            => Bytes.Wrap(stringSerDes.Serialize(key, new SerializationContext()));

        private byte[] CreateValue(ValueAndTimestamp<string> value)
            => valueAndTsSerDes.Serialize(value, new SerializationContext());

        private string FromValue(byte[] valueBytes)
            => stringSerDes.Deserialize(valueBytes, new SerializationContext());

        private Windowed<string> FromKey(byte[] keyBytes)
            => windowSerDes.Deserialize(keyBytes, new SerializationContext());


        [TearDown]
        public void End()
        {
            if (store != null)
            {
                store.Flush();
                stateManager.Close();
                recordCollector?.Close(false);
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

            store.Put(CreateKey("test"), CreateValue(ValueAndTimestamp<string>.Make("value", 0)), 0);
            store.Put(CreateKey("test2"), null, 0);

            var r = consumer.Consume();

            Assert.AreEqual("test", FromKey(r.Message.Key).Key);
            Assert.AreEqual("value", FromValue(r.Message.Value));

            r = consumer.Consume();

            Assert.AreEqual("test2", FromKey(r.Message.Key).Key);
            Assert.IsNull(r.Message.Value);

            consumer.Dispose();
        }

    }
}
