using Confluent.Kafka;
using Newtonsoft.Json;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KGroupedStreamAggTests
    {
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
                .Stream<string, string>("topic")
                .GroupByKey()
                .Aggregate(() => 0L, (k, v, agg) => agg + 1, m);

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
                .Stream<string, string>("topic")
                .GroupByKey()
                .Aggregate(() => 0, (k, v, agg) => agg + 1, m)
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
        public void AggAndQueryInStateStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-count";

            var builder = new StreamBuilder();

            var stream = builder
               .Stream<string, string>("topic")
               .GroupBy((k, v) => k.ToUpper());

            stream.Count(InMemory<string, long>.As("count-store"));
            stream.Aggregate(
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
                    InMemory<string, Dictionary<char, int>>.As("agg-store").WithValueSerdes(new DictionarySerDes())
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
                Assert.AreEqual(3, e);
            }
        }

        [Test]
        public void KeySerdesUnknow()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-agg";

            var builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .GroupBy((k, v) => k.ToCharArray()[0])
                .Aggregate(() => 0L, (k, v, agg) => agg + 1);

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
