using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KGroupedStreamCountTests
    {
        [Test]
        public void WithNullMaterialize()
        {
            // CERTIFIED THAT SAME IF Materialize is null, a state store exist for count processor with a generated namestore
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var serdes = new StringSerDes();

            config.ApplicationId = "test-count";
            config.UseRandomRocksDbConfigForTest();
            var builder = new StreamBuilder();
            Materialized<string, long, IKeyValueStore<Bytes, byte[]>> m = null;

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .Count(m);

            var topology = builder.Build();
            TaskId id = new TaskId { Id = 0, Partition = 0 };
            var processorTopology = topology.Builder.BuildTopology(id);

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);

            
            var part = new TopicPartition("topic", 0);
            StreamTask task = new StreamTask(
                "thread-0",
                id,
                new List<TopicPartition> { part },
                processorTopology,
                consumer,
                config,
                supplier,
                null,
                new MockChangelogRegister(),
                new StreamMetricsRegistry());
            task.InitializeStateStores();
            task.InitializeTopology();

            Assert.AreEqual(1, task.Context.States.StateStoreNames.Count());
            var nameStore = task.Context.States.StateStoreNames.ElementAt(0);
            Assert.IsNotNull(nameStore);
            Assert.AreNotEqual(string.Empty, nameStore);
            var store = task.GetStore(nameStore);
            Assert.IsInstanceOf<ITimestampedKeyValueStore<string, long>>(store);
            Assert.AreEqual(0, (store as ITimestampedKeyValueStore<string, long>).ApproximateNumEntries());
            config.RemoveRocksDbFolderForTest();
        }

        [Test]
        public void WithNullSerDes()
        {
            // WITH NULL SERDES, in running KeySerdes must be StringSerdes, and ValueSerdes Int64SerDes
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-count";

            var builder = new StreamBuilder();
            Materialized<string, long, IKeyValueStore<Bytes, byte[]>> m =
                InMemory.As<string, long>("count-store");

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .Count(m)
                .ToStream()
                .To("output-topic");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOuputTopic<string, long, StringSerDes, Int64SerDes>("output-topic");
                input.PipeInput("test", "1");
                input.PipeInput("test", "30");

                IEnumerable<KeyValuePair<string, long>> expected = new List<KeyValuePair<string, long>> {
                    KeyValuePair.Create("test", 1L),
                    KeyValuePair.Create("test", 2L)
                };

                var records = output.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();
                Assert.AreEqual(expected, records);
            }
        }

        [Test]
        public void CountAndQueryInStateStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-count";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupBy<char, CharSerDes>((k, v) => k.ToCharArray()[0])
                .Count(InMemory.As<char, long>("count-store").WithKeySerdes(new CharSerDes()));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1");
                input.PipeInput("test", "30");
                input.PipeInput("coucou", "120");
                var store = driver.GetKeyValueStore<char, long>("count-store");
                Assert.IsNotNull(store);
                Assert.AreEqual(2, store.ApproximateNumEntries());
                Assert.AreEqual(2, store.Get('t'));
                Assert.AreEqual(1, store.Get('c'));
            }
        }

        [Test]
        public void CountWithNullValue()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-count";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupBy<char, CharSerDes>((k, v) => k.ToCharArray()[0])
                .Count(InMemory.As<char, long>("count-store").WithKeySerdes(new CharSerDes()));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1");
                input.PipeInput("test", null);
                input.PipeInput("coucou", "120");
                var store = driver.GetKeyValueStore<char, long>("count-store");
                Assert.IsNotNull(store);
                Assert.AreEqual(2, store.ApproximateNumEntries());
                Assert.AreEqual(1, store.Get('t'));
                Assert.AreEqual(1, store.Get('c'));
            }
        }


        [Test]
        public void CountEmpty()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-count";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .Count(InMemory.As<string, long>("count-store"))
                .ToStream()
                .To("output");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOuputTopic<string, long, StringSerDes, Int64SerDes>("output");
                input.PipeInput("test", "1");
                var r = output.ReadKeyValue();
                Assert.AreEqual("test", r.Message.Key);
                Assert.AreEqual(1, r.Message.Value);
            }
        }

        [Test]
        public void CountWithName()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-count";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupByKey()
                .Count(InMemory.As<string, long>("count-store"), "count-01")
                .ToStream()
                .To("output");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOuputTopic<string, long, StringSerDes, Int64SerDes>("output");
                input.PipeInput("test", "1");
                var r = output.ReadKeyValue();
                Assert.AreEqual("test", r.Message.Key);
                Assert.AreEqual(1, r.Message.Value);
            }
        }

        [Test]
        public void KeySerdesUnknownWithParallel()
        {
            KeySerdesUnknown(true);
        }

        [Test]
        public void KeySerdesUnknownWithoutParallel()
        {
            KeySerdesUnknown(false);
        }
        
        private void KeySerdesUnknown(bool parallelProcessing)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-count",
                ParallelProcessing = parallelProcessing
            };

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupBy((k, v) => k.ToCharArray()[0])
                .Count(InMemory.As<char, long>("count-store"));

            var topology = builder.Build();
            Assert.Throws<StreamsException>(() =>
            {
                using var driver = new TopologyTestDriver(topology, config);
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1");
            });
        }
    }
}
