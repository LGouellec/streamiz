using System;
using System.Collections.Generic;
using System.IO;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.InMemory;
using static Streamiz.Kafka.Net.Processors.Internal.StoreChangelogReader;

namespace Streamiz.Kafka.Net.Tests.Private
{
    // TODO : 1.2.0, restore from recovery, restore from scratch, restore from offset invalid ...
    public class StoreChangelogReaderTests
    {
        private string stateDir;
        private string changelogTopic;
        private IStreamConfig config;
        private SyncKafkaSupplier supplier;
        private InMemoryKeyValueStore store;
        private StoreChangelogReader storeChangelogReader;
        private ProcessorStateManager stateMgr;

        [SetUp]
        public void Init()
        {
            stateDir = Path.Combine(".", Guid.NewGuid().ToString());
            config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-storechangelog-app";
            config.StateDir = stateDir;

            supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);
            var restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());

            var topicPart = new TopicPartition("topic", 0);
            changelogTopic = "store-changelog-topic";
            var changelogsTopics = new Dictionary<string, string>();
            changelogsTopics.Add("store", changelogTopic);
            var id = new TaskId
            {
                Id = 0,
                Partition = 0
            };

            store = new InMemoryKeyValueStore("store");
            storeChangelogReader = new StoreChangelogReader(config, restoreConsumer);
            stateMgr = new ProcessorStateManager(
                id,
                topicPart.ToSingle(),
                changelogsTopics,
                storeChangelogReader,
                new OffsetCheckpointFile(Path.Combine(config.StateDir, config.ApplicationId, $"{id.Id}-{id.Partition}"))
            );

            stateMgr.Register(store, (k, v) => store.Put(k, v));


            Message<byte[], byte[]> CreateMessage(string topic, string key, string value)
            {
                StringSerDes stringSerDes = new StringSerDes();
                return new Message<byte[], byte[]>
                {
                    Key = stringSerDes.Serialize(key, new SerializationContext(MessageComponentType.Key, topic)),
                    Value = stringSerDes.Serialize(value, new SerializationContext(MessageComponentType.Key, topic))
                };
            }

            producer.Produce(changelogTopic, CreateMessage(changelogTopic, "key1", "value1"));
            producer.Produce(changelogTopic, CreateMessage(changelogTopic, "key2", "value2"));
        }

        [TearDown]
        public void Dispose()
        {
            stateMgr.Close();
            storeChangelogReader.Clear();
        }

        [Test]
        public void RestoreComplete()
        {
            storeChangelogReader.Restore();

            Assert.AreEqual(2, store.ApproximateNumEntries());

            var metadata = storeChangelogReader.GetMetadata(new TopicPartition(changelogTopic, 0));
            Assert.IsNotNull(metadata);
            Assert.AreEqual(ChangelogState.COMPLETED, metadata.ChangelogState);
            Assert.AreEqual(2, metadata.RestoreEndOffset);
            Assert.AreEqual(2, metadata.TotalRestored);
        }


        [Test]
        public void RegisterFailed()
        {
            Assert.Throws<StreamsException>(() => storeChangelogReader.Register(new TopicPartition("test", 0), stateMgr));
        }
    }
}
