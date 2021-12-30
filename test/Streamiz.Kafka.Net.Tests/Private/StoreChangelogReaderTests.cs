using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.RocksDb;
using static Streamiz.Kafka.Net.Processors.Internal.StoreChangelogReader;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class StoreChangelogReaderTests
    {
        private string stateDir;
        private string changelogTopic;
        private IStreamConfig config;
        private SyncKafkaSupplier supplier;
        private RocksDbKeyValueStore store;
        private StoreChangelogReader storeChangelogReader;
        private ProcessorStateManager stateMgr;
        private ProcessorContext context;
        private IConsumer<byte[], byte[]> restoreConsumer;

        private Message<byte[], byte[]> CreateMessage(string topic, string key, string value)
        {
            StringSerDes stringSerDes = new StringSerDes();
            return new Message<byte[], byte[]>
            {
                Key = stringSerDes.Serialize(key, new SerializationContext(MessageComponentType.Key, topic)),
                Value = stringSerDes.Serialize(value, new SerializationContext(MessageComponentType.Key, topic))
            };
        }

        [SetUp]
        public void Init()
        {
            stateDir = Path.Combine(".", Guid.NewGuid().ToString());
            config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-storechangelog-app";
            config.StateDir = stateDir;
            config.PollMs = 100;

            supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());

            var topicPart = new TopicPartition("topic", 0);
            changelogTopic = "store-changelog-topic";
            var changelogsTopics = new Dictionary<string, string>();
            changelogsTopics.Add("store", changelogTopic);
            var id = new TaskId
            {
                Id = 0,
                Partition = 0
            };

            store = new RocksDbKeyValueStore("store");
            storeChangelogReader = new StoreChangelogReader(config, restoreConsumer);
            stateMgr = new ProcessorStateManager(
                id,
                topicPart.ToSingle(),
                changelogsTopics,
                storeChangelogReader,
                new OffsetCheckpointFile(Path.Combine(config.StateDir, config.ApplicationId, $"{id.Id}-{id.Partition}"))
            );

            Mock<AbstractTask> moq = new Mock<AbstractTask>();
            moq.Setup(t => t.Id).Returns(new TaskId { Id = 0, Partition = 0 });

            context = new ProcessorContext(moq.Object, config, stateMgr);
            store.Init(context, store);

            producer.Produce(changelogTopic, CreateMessage(changelogTopic, "key1", "value1"));
            producer.Produce(changelogTopic, CreateMessage(changelogTopic, "key2", "value2"));
        }

        [TearDown]
        public void Dispose()
        {
            stateMgr.Close();
            storeChangelogReader.Clear();
            Directory.Delete(config.StateDir, true);
        }

        [Test]
        public void RestoreComplete()
        {
            storeChangelogReader.Restore();

            Assert.AreEqual(2, store.All().ToList().Count);

            var metadata = storeChangelogReader.GetMetadata(new TopicPartition(changelogTopic, 0));
            Assert.IsNotNull(metadata);
            Assert.AreEqual(ChangelogState.COMPLETED, metadata.ChangelogState);
            Assert.AreEqual(1, metadata.RestoreEndOffset);
            Assert.AreEqual(2, metadata.TotalRestored);
        }

        [Test]
        public void RestoreFromRecovery()
        {
            storeChangelogReader.Restore();
            stateMgr.Checkpoint();
            stateMgr.Close();
            store.Init(context, store);
            stateMgr.InitializeOffsetsFromCheckpoint();

            var producer = supplier.GetProducer(config.ToProducerConfig());
            producer.Produce(changelogTopic, CreateMessage(changelogTopic, "key3", "value3"));
            producer.Produce(changelogTopic, CreateMessage(changelogTopic, "key4", "value4"));

            restoreConsumer.Resume(restoreConsumer.Assignment);

            storeChangelogReader.Restore();
            stateMgr.Checkpoint();
            stateMgr.Close();

            File.Delete(Path.Combine(config.StateDir, config.ApplicationId, "0-0", ".checkpoint"));

            store.Init(context, store);
            stateMgr.InitializeOffsetsFromCheckpoint();

            restoreConsumer.Resume(restoreConsumer.Assignment);

            storeChangelogReader.Restore();

            Assert.AreEqual(4, store.All().ToList().Count);

            var metadata = storeChangelogReader.GetMetadata(new TopicPartition(changelogTopic, 0));
            Assert.IsNotNull(metadata);
            Assert.AreEqual(ChangelogState.COMPLETED, metadata.ChangelogState);
            Assert.AreEqual(3, metadata.RestoreEndOffset);
            Assert.AreEqual(2, metadata.TotalRestored);
        }

        [Test]
        public void RestoreFromScratch()
        {
            storeChangelogReader.Restore();
            stateMgr.Checkpoint();
            stateMgr.Close();

            var producer = supplier.GetProducer(config.ToProducerConfig());
            producer.Produce(changelogTopic, CreateMessage(changelogTopic, "key3", "value3"));
            producer.Produce(changelogTopic, CreateMessage(changelogTopic, "key4", "value4"));

            // delete state dir rocksdb
            Directory.Delete(Path.Combine(config.StateDir), true);

            restoreConsumer.Resume(restoreConsumer.Assignment);
            store.Init(context, store);
            stateMgr.InitializeOffsetsFromCheckpoint();

            storeChangelogReader.Restore();

            Assert.AreEqual(4, store.All().ToList().Count);

            var metadata = storeChangelogReader.GetMetadata(new TopicPartition(changelogTopic, 0));
            Assert.IsNotNull(metadata);
            Assert.AreEqual(ChangelogState.COMPLETED, metadata.ChangelogState);
            Assert.AreEqual(3, metadata.RestoreEndOffset);
            Assert.AreEqual(4, metadata.TotalRestored);
        }

        [Test]
        public void RestoreFromInvalidOffset()
        {
            storeChangelogReader.Restore();
            stateMgr.Checkpoint();
            stateMgr.Close();

            // set an invalid offset in checkpoint file
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("0").AppendLine("1").AppendLine("store-changelog-topic 0 -100");
            File.WriteAllText(Path.Combine(config.StateDir, config.ApplicationId, "0-0", ".checkpoint"), sb.ToString());

            restoreConsumer.Resume(restoreConsumer.Assignment);
            store.Init(context, store);
            stateMgr.InitializeOffsetsFromCheckpoint();

            storeChangelogReader.Restore();

            Assert.AreEqual(2, store.All().ToList().Count);

            var metadata = storeChangelogReader.GetMetadata(new TopicPartition(changelogTopic, 0));
            Assert.IsNotNull(metadata);
            Assert.AreEqual(ChangelogState.COMPLETED, metadata.ChangelogState);
            Assert.AreEqual(1, metadata.RestoreEndOffset);
            Assert.AreEqual(2, metadata.TotalRestored);
        }

        [Test]
        public void RegisterFailed()
        {
            Assert.Throws<StreamsException>(() => storeChangelogReader.Register(new TopicPartition("test", 0), stateMgr));
        }
    }
}
