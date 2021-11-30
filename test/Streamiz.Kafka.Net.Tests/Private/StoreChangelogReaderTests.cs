using System;
using System.Collections.Generic;
using System.IO;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.InMemory;

namespace Streamiz.Kafka.Net.Tests.Private
{
    // TODO : finish
    public class StoreChangelogReaderTests
    {
        private string stateDir;
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
            var changelogsTopics = new Dictionary<string, string>();
            changelogsTopics.Add("store", "store-changelog-topic");
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

            stateMgr.Register(store, (k, v) => Console.WriteLine($"{k}-{v}"));
        }

        [TearDown]
        public void Dispose()
        {
            stateMgr.Close();
            storeChangelogReader.Clear();
        }

        [Test]
        public void test()
        {
            storeChangelogReader.Restore();
        }
    }
}
