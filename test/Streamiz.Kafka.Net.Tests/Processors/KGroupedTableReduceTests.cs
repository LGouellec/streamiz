﻿using System;
using System.Collections.Generic;
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
    public class KGroupedTableReduceTests
    {
        public class MyAddReducer : Reducer<string>
        {
            public string Apply(string value1, string value2)
                =>
                (!string.IsNullOrEmpty(value2) && !string.IsNullOrEmpty(value1)) ? (value2.Length > value1.Length ? value2 : value1) : "";
        }

        public class MySubReducer : Reducer<string>
        {
            public string Apply(string value1, string value2) => value2;
        }

        [Test]
        public void WithNullMaterialize()
        {
            // CERTIFIED THAT SAME IF Materialize is null, a state store exist for count processor with a generated namestore
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var serdes = new StringSerDes();

            config.ApplicationId = "test-reduce";
            config.UseRandomRocksDbConfigForTest();
            
            var builder = new StreamBuilder();
            Materialized<string, int, IKeyValueStore<Bytes, byte[]>> m = null;

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .MapValues((v, _) => v.Length)
                .GroupBy((k,v, _) => KeyValuePair.Create(k.ToUpper(),v))
                .Reduce((v1, v2) => Math.Max(v1, v2), (v1, v2) => v2, m);

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
            config.ApplicationId = "test-reduce";

            var builder = new StreamBuilder();
            Materialized<string, int, IKeyValueStore<Bytes, byte[]>> m =
                InMemory.As<string, int>("reduce-store");

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .MapValues((v, _) => v.Length)
                .GroupBy((k, v, _) => KeyValuePair.Create(k.ToUpper(), v))
                .Reduce((v1, v2) => Math.Max(v1, v2), (v1,v2) => v2, m)
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
        public void WithNullReducer()
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
                    .Reduce((Reducer<int>)null, (Reducer<int>)null, m);
            });
        }

        [Test]
        public void ReduceAndQueryInStateStore()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-reduce";

            var builder = new StreamBuilder();

            var table = builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .MapValues((v, _) => v.Length)
                .GroupBy<string, int, StringSerDes, Int32SerDes>((k, v, _) => KeyValuePair.Create(k.ToUpper(), v));

            table.Count(InMemory.As<string, long>("count-store"));
            table.Reduce(
                    (v1, v2) => Math.Max(v1, v2),
                    (v1, v2) => Math.Max(v1, v2),
                    InMemory.As<string, int>("reduce-store").WithValueSerdes<Int32SerDes>());

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1");
                input.PipeInput("test", "120");
                input.PipeInput("test", "30");
                input.PipeInput("coucou", "120");

                var store = driver.GetKeyValueStore<string, int>("reduce-store");
                Assert.IsNotNull(store);
                Assert.AreEqual(2, store.ApproximateNumEntries());
                var el = store.Get("TEST");
                Assert.IsNotNull(el);
                Assert.AreEqual(3, el);

                var storeCount = driver.GetKeyValueStore<string, long>("count-store");
                Assert.IsNotNull(storeCount);
                Assert.AreEqual(2, store.ApproximateNumEntries());
                var e = storeCount.Get("TEST");
                Assert.IsNotNull(e);
                Assert.AreEqual(1, e);
            }
        }

        [Test]
        public void Reduce2()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-reduce";

            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .GroupBy((k, v, _) => KeyValuePair.Create(k.ToUpper(), v))
                   .Reduce(
                        (v1, v2) => v2.Length > v1.Length ? v2 : v1,
                        (v1, v2) => v2,
                        InMemory.As<string, string>("reduce-store"));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1");
                input.PipeInput("test", "12");

                var store = driver.GetKeyValueStore<string, string>("reduce-store");
                Assert.IsNotNull(store);
                Assert.AreEqual(1, store.ApproximateNumEntries());
                var el = store.Get("TEST");
                Assert.IsNotNull(el);
                Assert.AreEqual("12", el);
            }
        }

        [Test]
        public void Reduce3()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-reduce";

            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .GroupBy((k, v, _) => KeyValuePair.Create(k.ToUpper(), v))
               .Reduce(new MyAddReducer(), new MySubReducer(), InMemory.As<string, string>("reduce-store"));

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "15151500");
                input.PipeInput("test", "1200");

                var store = driver.GetKeyValueStore<string, string>("reduce-store");
                Assert.IsNotNull(store);
                Assert.AreEqual(1, store.ApproximateNumEntries());
                var el = store.Get("TEST");
                Assert.IsNotNull(el);
                Assert.AreEqual("15151500", el);
            }
        }

        [Test]
        public void Reduce4()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-count";

            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .GroupBy((k, v, _) => KeyValuePair.Create(k.ToUpper(), v))
               .Reduce(
                    new MyAddReducer(), new MySubReducer(),
                    InMemory.As<string, string>("reduce-store"),
                    "reduce-processor");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1");
                input.PipeInput("test", null);
                input.PipeInput("test", "12");

                var store = driver.GetKeyValueStore<string, string>("reduce-store");
                Assert.IsNotNull(store);
                // null doesn't matter
                Assert.AreEqual(1, store.ApproximateNumEntries());
                var el = store.Get("TEST");
                Assert.IsNotNull(el);
                Assert.AreEqual("12", el);
            }
        }

        [Test]
        public void Reduce5()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-count";

            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .GroupBy((k, v, _) => KeyValuePair.Create(k?.ToUpper(), v))
               .Reduce(
                    new MyAddReducer(), new MySubReducer(),
                    InMemory.As<string, string>("reduce-store"),
                    "reduce-processor");

            var topology = builder.Build();
            using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("topic");
                input.PipeInput("test", "1");
                input.PipeInput("test", null);
                input.PipeInput(null, "34");
                input.PipeInput(null, null);
                input.PipeInput("test", "12");

                var store = driver.GetKeyValueStore<string, string>("reduce-store");
                Assert.IsNotNull(store);
                // null doesn't matter
                Assert.AreEqual(1, store.ApproximateNumEntries());
                var el = store.Get("TEST");
                Assert.IsNotNull(el);
                Assert.AreEqual("12", el);
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
                ApplicationId = "test-reduce",
                ParallelProcessing = parallelProcessing
            };

            var builder = new StreamBuilder();

            builder
                .Table<string, string>("topic", InMemory.As<string, string>())
                .GroupBy((k, v, _) => KeyValuePair.Create(k.ToCharArray()[0], v))
                .Reduce(
                    (v1, v2) => v2,
                    (v1, v2) => v2,
                    InMemory.As<char, string>("reduce-store"));

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
