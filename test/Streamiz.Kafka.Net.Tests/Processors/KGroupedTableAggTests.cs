using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KGroupedTableAggTests
    {
        internal class MyInitializer : Initializer<Dictionary<char, int>>
        {
            public Dictionary<char, int> Apply() => new Dictionary<char, int>();
        }

        internal class MyAdderAggregator : Aggregator<string, string, Dictionary<char, int>>
        {
            public Dictionary<char, int> Apply(string key, string value, Dictionary<char, int> aggregate)
            {
                var caracs = value.ToCharArray();
                foreach (var c in caracs)
                {
                    if (aggregate.ContainsKey(c))
                        ++aggregate[c];
                    else
                        aggregate.Add(c, 1);
                }
                return aggregate;
            }
        }

        internal class MySubAggregator : Aggregator<string, string, Dictionary<char, int>>
        {
            public Dictionary<char, int> Apply(string key, string value, Dictionary<char, int> aggregate)
            {
                return aggregate;
            }
        }

        internal class DictionarySerDes : AbstractSerDes<Dictionary<char, int>>
        {
            public override Dictionary<char, int> Deserialize(byte[] data, SerializationContext context)
            {
                var s = Encoding.UTF8.GetString(data);
                return JsonConvert.DeserializeObject<Dictionary<char, int>>(s);
            }

            public override byte[] Serialize(Dictionary<char, int> data, SerializationContext context)
            {
                return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data, Formatting.Indented));
            }
        }

        [Test]
        public void WithNullMaterialize()
        {
            // CERTIFIED THAT SAME IF Materialize is null, a state store exist for count processor with a generated namestore
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var serdes = new StringSerDes();

            config.ApplicationId = "test-agg";
            config.UseRandomRocksDbConfigForTest();

            var builder = new StreamBuilder();
            Materialized<string, long, IKeyValueStore<Bytes, byte[]>> m = null;

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .GroupBy((k, v, _) => KeyValuePair.Create(k.ToUpper(), v))
                .Aggregate(() => 0L, (k, v, agg) => agg + 1, (k, v, agg) => agg, m);

            var topology = builder.Build();
            Assert.Throws<StreamsException>(() =>
            {
                using (var driver = new TopologyTestDriver(topology, config))
                {
                    var input = driver.CreateInputTopic<string, string>("topic");
                    input.PipeInput("test", "1");
                }
            });
            config.RemoveRocksDbFolderForTest();
        }

        [Test]
        public void WithNullSerDes()
        {
            // WITH NULL SERDES, in running KeySerdes must be StringSerdes, and ValueSerdes Int64SerDes
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-agg";

            var builder = new StreamBuilder();
            Materialized<string, long, IKeyValueStore<Bytes, byte[]>> m =
                InMemory.As<string, long>("agg-store");

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .GroupBy((k, v, _) => KeyValuePair.Create(k.ToUpper(), v))
                .Aggregate(() => 0, (k, v, agg) => agg + 1, (k, v, agg) => agg, m)
                .ToStream()
                .To("output-topic");

            var topology = builder.Build();
            Assert.Throws<StreamsException>(() =>
            {
                using (var driver = new TopologyTestDriver(topology, config))
                {
                    var input = driver.CreateInputTopic<string, string>("topic");
                    input.PipeInput("test", "1");
                }
            });
        }

        [Test]
        public void WithNullAggregator()
        {
            // WITH NULL SERDES, in running KeySerdes must be StringSerdes, and ValueSerdes Int64SerDes
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-reduce";

            var builder = new StreamBuilder();
            Materialized<string, int, IKeyValueStore<Bytes, byte[]>> m =
                InMemory.As<string, int>("reduce-store");

            Assert.Throws<ArgumentNullException>(() =>
            {
                builder
                    .Table<string, string>("topic", InMemory.As<string, string>())
                    .MapValues((v, _) => v.Length)
                    .GroupBy((k, v, _) => KeyValuePair.Create(k.ToUpper(), v))
                    .Aggregate((Initializer<int>)null, (Aggregator<string, int, int>)null, (Aggregator<string,int,int>)null, m);
            });
        }

        [Test]
        public void AggAndQueryInStateStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-agg";

            var builder = new StreamBuilder();

            var table = builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .GroupBy((k, v, _) => KeyValuePair.Create(k.ToUpper(), v));

            table.Count(InMemory.As<string, long>("count-store"));
            table.Aggregate(
                    () => new Dictionary<char, int>(),
                    (k, v, old) =>
                    {
                        var caracs = v.ToCharArray();
                        foreach (var c in caracs)
                        {
                            if (old.ContainsKey(c))
                                ++old[c];
                            else
                                old.Add(c, 1);
                        }
                        return old;
                    },
                    (k, v, old) => old,
                    InMemory.As<string, Dictionary<char, int>>("agg-store").WithValueSerdes<DictionarySerDes>()
                );

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                Dictionary<char, int> testExpected = new Dictionary<char, int>
                {
                    {'1', 2 },
                    {'2', 1 },
                    {'3', 1 },
                    {'0', 1 },
                };
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1");
                input.PipeInput("test", "12");
                input.PipeInput("test", "30");
                input.PipeInput("coucou", "120");

                var store = driver.GetKeyValueStore<string, Dictionary<char, int>>("agg-store");
                Assert.IsNotNull(store);
                Assert.AreEqual(2, store.ApproximateNumEntries());
                var el = store.Get("TEST");
                Assert.IsNotNull(el);
                Assert.AreEqual(testExpected, el);

                var storeCount = driver.GetKeyValueStore<string, long>("count-store");
                Assert.IsNotNull(storeCount);
                Assert.AreEqual(2, store.ApproximateNumEntries());
                var e = storeCount.Get("TEST");
                Assert.IsNotNull(e);
                Assert.AreEqual(1, e);
            }
        }

        [Test]
        public void Agg2()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-agg";
            config.UseRandomRocksDbConfigForTest();
            
            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .GroupBy((k, v, _) => KeyValuePair.Create(k.ToUpper(), v))
                .Aggregate<Dictionary<char, int>, DictionarySerDes>(
                    () => new Dictionary<char, int>(),
                    (k, v, old) =>
                    {
                        var caracs = v.ToCharArray();
                        foreach (var c in caracs)
                        {
                            if (old.ContainsKey(c))
                                ++old[c];
                            else
                                old.Add(c, 1);
                        }
                        return old;
                    },
                    (k, v, old) => old
                );

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                Dictionary<char, int> testExpected = new Dictionary<char, int>
                {
                    {'1', 2 },
                    {'2', 1 }
                };
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOutputTopic<Dictionary<char, int>, DictionarySerDes>("output");
                input.PipeInput("test", "1");
                input.PipeInput("test", "12");

                var store = driver.GetKeyValueStore<string, Dictionary<char, int>>("KTABLE-AGGREGATE-STATE-STORE-0000000005");
                Assert.IsNotNull(store);
                
                var el = store.Get("TEST");
                Assert.IsNotNull(el);
                Assert.AreEqual(testExpected, el);
            }

            config.RemoveRocksDbFolderForTest();
        }

        [Test]
        public void Agg3()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-agg";
            config.UseRandomRocksDbConfigForTest();
            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .GroupBy((k, v, _) => KeyValuePair.Create(k.ToUpper(), v))
                .Aggregate<Dictionary<char, int>, DictionarySerDes>(
                    new MyInitializer(),
                    new MyAdderAggregator(),
                    new MySubAggregator()
                );

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                Dictionary<char, int> testExpected = new Dictionary<char, int>
                {
                    {'1', 2 },
                    {'2', 1 }
                };
                var input = driver.CreateInputTopic<string, string>("topic");
                var output = driver.CreateOutputTopic<Dictionary<char, int>, DictionarySerDes>("output");
                input.PipeInput("test", "1");
                input.PipeInput("test", "12");

                var store = driver.GetKeyValueStore<string, Dictionary<char, int>>("KTABLE-AGGREGATE-STATE-STORE-0000000005");
                Assert.IsNotNull(store);
                
                var el = store.Get("TEST");
                Assert.IsNotNull(el);
                Assert.AreEqual(testExpected, el);
            }

            config.RemoveRocksDbFolderForTest();
        }

        [Test]
        public void Agg4()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-agg";

            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .GroupBy((k, v, _) => KeyValuePair.Create(k.ToUpper(), v))
                .Aggregate(
                    () => 0L,
                    (k, v, agg) => agg + 1,
                    (k, v, agg) => agg,
                    InMemory.As<string, long>("agg-store").WithValueSerdes<Int64SerDes>());

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1");
                input.PipeInput("test", null);
                input.PipeInput("test", "12");

                var store = driver.GetKeyValueStore<string, long>("agg-store");
                Assert.IsNotNull(store);
                // null doesn't matter
                Assert.AreEqual(1, store.ApproximateNumEntries());
                var el = store.Get("TEST");
                Assert.IsNotNull(el);
                Assert.AreEqual(2, el);
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
                ApplicationId = "test-agg",
                ParallelProcessing = parallelProcessing
            };

            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .GroupBy((k, v, _) => KeyValuePair.Create(k.ToCharArray()[0], v))
                .Aggregate(
                    () => 0L, 
                    (k, v, agg) => agg + 1,
                    (k, v, agg) => agg,
                    InMemory.As<char, long>());

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
