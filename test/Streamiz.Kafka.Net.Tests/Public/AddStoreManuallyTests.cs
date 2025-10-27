using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class AddStoreManuallyTests
    {
        private class TTransformer : ITransformer<string, string, string, string>
        {
            private readonly bool _deduplication;
            private IKeyValueStore<string, string> stateStore;

            public TTransformer():this(true){}
            
            public TTransformer(bool deduplication)
            {
                _deduplication = deduplication;
            }

            public void Init(ProcessorContext<string, string> context)
            {
                stateStore = (IKeyValueStore<string, string>)context.GetStateStore("output-store");
            }

            public Record<string, string> Process(Record<string, string> record)
            {
                // De-duplication process
                if (_deduplication)
                {
                    if (stateStore.Get(record.Key) == null)
                    {
                        stateStore.Put(record.Key, record.Value);
                        return record;
                    }

                    return null;
                }
                else // it's a join
                {
                    if (stateStore.Get(record.Key) != null)
                        
                        return record;
                    return null;
                }
            }

            public void Close()
            {
                
            }
        }

        private class TProcessor : IProcessor<string, string>
        {
            private IKeyValueStore<string, string> stateStore;

            public void Init(ProcessorContext<string, string> context)
            {
                stateStore = (IKeyValueStore<string, string>)context.GetStateStore("output-store");
            }

            public void Process(Record<string, string> record)
            {
                stateStore.Put(record.Key, record.Value);
            }

            public void Close()
            {
                throw new System.NotImplementedException();
            }
        }
        
        [Test]
        public void AddOneStoreManually()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-manually-state-store";
            
            var builder = new StreamBuilder();
            var storeBuilder = State.Stores.KeyValueStoreBuilder(
                    State.Stores.InMemoryKeyValueStore("output-store"),
                    new StringSerDes(),
                    new StringSerDes())
                .WithLoggingDisabled();

            builder.AddStateStore(storeBuilder);
            
            builder.Stream<string, string>("input-topic")
                .Transform(
                    TransformerBuilder
                        .New<string, string, string, string>()
                        .Transformer<TTransformer>()
                        .Build(),
                    storeNames: "output-store")
                .To("output-topic");

            var s = builder.Build().Describe().ToString();
            
            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("input-topic");
                var outputTopic = driver.CreateOutputTopic<string, string>("output-topic");

                inputTopic.PipeInput("key1", "value1");
                inputTopic.PipeInput("key2", "value1");
                inputTopic.PipeInput("key1", "value2");
                inputTopic.PipeInput("key3", "value1");
                
                var expected = new List<(string, string)>();
                expected.Add(("key1", "value1"));
                expected.Add(("key2", "value1"));
                expected.Add(("key3", "value1"));

                var list = outputTopic.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();

                Assert.AreEqual(expected, list);
            }
        }

        [Test]
        public void AddGlobalStoreManually()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-manually-state-store";
            
            var builder = new StreamBuilder();
            var storeBuilder = State.Stores.KeyValueStoreBuilder(
                State.Stores.InMemoryKeyValueStore("output-store"),
                new StringSerDes(),
                new StringSerDes());

            builder.AddGlobalStore(storeBuilder, 
                "global-topic",
                ProcessorBuilder.New<string, string>()
                    .Processor<TProcessor>()
                    .Build(),
                new StringSerDes(),
                new StringSerDes());
            
            builder.Stream<string, string>("input-topic")
                .Transform(
                    TransformerBuilder
                        .New<string, string, string, string>()
                        .Transformer<TTransformer>(false)
                        .Build())
                .To("output-topic");
            
            Topology t = builder.Build();

            using var driver = new TopologyTestDriver(t, config);
            var globalTopic = driver.CreateInputTopic<string, string>("global-topic");
            var inputTopic = driver.CreateInputTopic<string, string>("input-topic");
            var outputTopic = driver.CreateOutputTopic<string, string>("output-topic");

            globalTopic.PipeInput("key1", "value1");
            globalTopic.PipeInput("key2", "value1");
            globalTopic.PipeInput("key3", "value1");
            
            inputTopic.PipeInput("key1", "value1");
            inputTopic.PipeInput("key2", "value1");
            inputTopic.PipeInput("key1", "value2");
            inputTopic.PipeInput("key3", "value1");
                
            var expected = new List<(string, string)>();
            expected.Add(("key1", "value1"));
            expected.Add(("key2", "value1"));
            expected.Add(("key1", "value2"));
            expected.Add(("key3", "value1"));

            var list = outputTopic.ReadKeyValueList().Select(r => (r.Message.Key, r.Message.Value)).ToList();

            Assert.AreEqual(expected, list);
        }
        
        [Test]
        public void AddGlobalStoreManuallyWrongBuilder()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-manually-state-store";
            
            var builder = new StreamBuilder();
            var storeBuilder = State.Stores.TimestampedWindowStoreBuilder(
                State.Stores.InMemoryWindowStore("output-store", TimeSpan.FromDays(1), TimeSpan.FromDays(1)),
                new StringSerDes(),
                new StringSerDes());

            Assert.Throws<TopologyException>(() => builder.AddGlobalStore(storeBuilder,
                "global-topic",
                ProcessorBuilder.New<string, string>()
                    .Processor<TProcessor>()
                    .Build(),
                new StringSerDes(),
                new StringSerDes()));
        }

        [Test]
        public void AddGlobalStoreManuallyAndRestoreOnProcess()
        {
            StreamBuilder GetTopology()
            {
                var builder = new StreamBuilder();
                var storeBuilder = State.Stores.KeyValueStoreBuilder(
                    State.Stores.InMemoryKeyValueStore("output-store"),
                    new StringSerDes(),
                    new StringSerDes());

                builder.AddGlobalStore(storeBuilder,
                    "global-topic",
                    ProcessorBuilder.New<string, string>()
                        .Processor<TProcessor>()
                        .Build(),
                    new StringSerDes(),
                    new StringSerDes());

                builder.Stream<string, string>("input-topic")
                    .Transform(
                        TransformerBuilder
                            .New<string, string, string, string>()
                            .Transformer<TTransformer>(false)
                            .Build())
                    .To("output-topic");
                return builder;
            }

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-manually-state-store";

            var kafkaSupplier = new MockKafkaSupplier(3, 0, false);

            Topology t = GetTopology().Build();

            using (var driver = new TopologyTestDriver(t, config, kafkaSupplier)){
                var inputTopic = driver.CreateInputTopic<string, string>("input-topic");
                var outputTopic = driver.CreateOutputTopic<string, string>("output-topic");
                var globalTopic = driver.CreateInputTopic<string, string>("global-topic");

                globalTopic.PipeInput("key1", "value1");
                globalTopic.PipeInput("key2", "value1");
                globalTopic.PipeInput("key3", "value1");

                Thread.Sleep(200);
                
                DateTime now = DateTime.Now;
                inputTopic.PipeInput("key1", "value1", now);
                inputTopic.PipeInput("key2", "value1", now.AddSeconds(1));
                inputTopic.PipeInput("key1", "value2", now.AddSeconds(2));
                inputTopic.PipeInput("key3", "value1", now.AddSeconds(3));

                var expected = new List<(string, string)>();
                expected.Add(("key1", "value1"));
                expected.Add(("key2", "value1"));
                expected.Add(("key1", "value2"));
                expected.Add(("key3", "value1"));

                var records = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(outputTopic, 4);
                var list = records.Select(r => (r.Message.Key, r.Message.Value, r.Message.Timestamp.UnixTimestampMs))
                    .OrderBy(e => e.UnixTimestampMs)
                    .Select(e => (e.Key, e.Value))
                    .ToList();

                Assert.AreEqual(expected, list);
            }

            t = GetTopology().Build();

            using (var driver2 = new TopologyTestDriver(t, config, kafkaSupplier))
            {
                var inputTopic = driver2.CreateInputTopic<string, string>("input-topic");
                var outputTopic = driver2.CreateOutputTopic<string, string>("output-topic");

                // check the manual global store has been restored successfully
                inputTopic.PipeInput("key1", "value1");

                var expected2 = new List<(string, string)>();
                expected2.Add(("key1", "value1"));

                var records = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(outputTopic, 1);
                var list2 = records.Select(r => (r.Message.Key, r.Message.Value)).ToList();
                
                Assert.AreEqual(expected2, list2);
            }
        }
    }
}