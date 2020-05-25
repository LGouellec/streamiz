using Newtonsoft.Json;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Text;

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
            public override Dictionary<char, int> Deserialize(byte[] data)
            {
                var s = Encoding.UTF8.GetString(data);
                return JsonConvert.DeserializeObject<Dictionary<char, int>>(s);
            }

            public override byte[] Serialize(Dictionary<char, int> data)
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

            var builder = new StreamBuilder();
            Materialized<string, long, IKeyValueStore<Bytes, byte[]>> m = null;

            builder
                .Table<string, string>("topic")
                .GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v))
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
        }

        [Test]
        public void WithNullSerDes()
        {
            // WITH NULL SERDES, in running KeySerdes must be StringSerdes, and ValueSerdes Int64SerDes
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-agg";

            var builder = new StreamBuilder();
            Materialized<string, long, IKeyValueStore<Bytes, byte[]>> m =
                Materialized<string, long, IKeyValueStore<Bytes, byte[]>>
                    .Create("agg-store")
                    .With(null, null);

            builder
                .Table<string, string>("topic")
                .GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v))
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
                Materialized<string, int, IKeyValueStore<Bytes, byte[]>>
                    .Create("reduce-store")
                    .With(null, null);

            Assert.Throws<ArgumentNullException>(() =>
            {
                builder
                    .Table<string, string>("topic")
                    .MapValues((v) => v.Length)
                    .GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v))
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
                .Table<string, string>("topic")
                .GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v));

            table.Count(InMemory<string, long>.As("count-store"));
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
                    InMemory<string, Dictionary<char, int>>.As("agg-store").WithValueSerdes<DictionarySerDes>()
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

            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic")
                .GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v))
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
                var output = driver.CreateOuputTopic<Dictionary<char, int>, DictionarySerDes>("output");
                input.PipeInput("test", "1");
                input.PipeInput("test", "12");

                var store = driver.GetKeyValueStore<string, Dictionary<char, int>>("KTABLE-AGGREGATE-STATE-STORE-0000000006");
                Assert.IsNotNull(store);
                Assert.AreEqual(1, store.ApproximateNumEntries());
                var el = store.Get("TEST");
                Assert.IsNotNull(el);
                Assert.AreEqual(testExpected, el);
            }
        }

        [Test]
        public void Agg3()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-agg";

            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic")
                .GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v))
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
                var output = driver.CreateOuputTopic<Dictionary<char, int>, DictionarySerDes>("output");
                input.PipeInput("test", "1");
                input.PipeInput("test", "12");

                var store = driver.GetKeyValueStore<string, Dictionary<char, int>>("KTABLE-AGGREGATE-STATE-STORE-0000000006");
                Assert.IsNotNull(store);
                Assert.AreEqual(1, store.ApproximateNumEntries());
                var el = store.Get("TEST");
                Assert.IsNotNull(el);
                Assert.AreEqual(testExpected, el);
            }
        }

        [Test]
        public void Agg4()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-agg";

            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic")
                .GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v))
                .Aggregate(
                    () => 0L,
                    (k, v, agg) => agg + 1,
                    (k, v, agg) => agg,
                    InMemory<string, long>.As("agg-store").WithValueSerdes<Int64SerDes>());

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
        public void KeySerdesUnknow()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-agg";

            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic")
                .GroupBy((k, v) => KeyValuePair.Create(k.ToCharArray()[0], v))
                .Aggregate(() => 0L, (k, v, agg) => agg + 1, (k, v, agg) => agg);

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
    }
}
