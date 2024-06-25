using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamTransformerAPITests
    {
        private class MyTransformer : ITransformer<string, string, string, string>
        {
            private ProcessorContext<string,string> context;

            public MyTransformer()
            {
            }
            
            public void Init(ProcessorContext<string, string> context)
            {
                this.context = context;
            }

            public Record<string, string> Process(Record<string, string> record)
            {
                return Record<string, string>.Create(record.Key, record.Value.ToUpper());
            }

            public void Close()
            {
                
            }
        }

        private class MyStatefulTransformer : ITransformer<string, string, string, string>
        {
            private string storeName;
            private IKeyValueStore<string,string> store;

            public MyStatefulTransformer()
            {
                
            }
            public MyStatefulTransformer(string storeName)
            {
                this.storeName = storeName;
            }
            
            public void Init(ProcessorContext<string, string> context)
            {
                store = (IKeyValueStore<string, string>)context.GetStateStore(storeName);
            }

            public Record<string, string> Process(Record<string, string> record)
            {
                store.Put(record.Key, record.Value);
                return Record<string, string>.Create(record.Key, record.Value.ToUpper());
            }

            public void Close()
            {
                
            }
        }

        [Test]
        public void TransformerAPILambda()
        {
            var builder = new StreamBuilder();
            
            builder.Stream<string, string>("topic")
                .Transform(TransformerBuilder
                    .New<string, string, string, string>()
                    .Transformer((record) => Record<string, string>.Create(record.Key, record.Value.ToUpper()))
                    .Build())
                .To("topic-output");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-transformer-api";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("key1", "value1");
                inputTopic.PipeInput("key2", "value2");

                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                var mapRecords = outputTopic.ReadKeyValuesToMap();
                Assert.AreEqual(2, mapRecords.Count);
                Assert.AreEqual("VALUE1", mapRecords["key1"]);
                Assert.AreEqual("VALUE2", mapRecords["key2"]);
            }
        }
        
        [Test]
        public void TransformerAPIProcessor()
        {
            var builder = new StreamBuilder();
            var data = new List<(string, string)>();
            
            builder.Stream<string, string>("topic")
                .Transform(TransformerBuilder
                    .New<string, string, string, string>()
                    .Transformer<MyTransformer>()
                    .Build())
                .To("topic-output");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-transformer-api";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("key1", "value1");
                inputTopic.PipeInput("key2", "value2");

                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                var mapRecords = outputTopic.ReadKeyValuesToMap();
                Assert.AreEqual(2, mapRecords.Count);
                Assert.AreEqual("VALUE1", mapRecords["key1"]);
                Assert.AreEqual("VALUE2", mapRecords["key2"]);
            }
        }

        [Test]
        public void TransformerAPIStatefull()
        {
            var builder = new StreamBuilder();
            
            builder.Stream<string, string>("topic")
                .Transform(TransformerBuilder
                    .New<string, string, string, string>()
                    .Transformer<MyStatefulTransformer>("my-store")
                    .StateStore(State.Stores.KeyValueStoreBuilder(
                            State.Stores.InMemoryKeyValueStore("my-store"),
                            new StringSerDes(),
                            new StringSerDes()))
                    .Build())
                .To("topic-output");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-transformer-api";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput(null, "value");
                inputTopic.PipeInput("key1", "value1");
                inputTopic.PipeInput("key2", "value2");

                var store = driver.GetKeyValueStore<string, string>("my-store");
                Assert.AreEqual(2, store.ApproximateNumEntries());
                Assert.AreEqual("value1", store.Get("key1"));
                Assert.AreEqual("value2", store.Get("key2"));
                
                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                var mapRecords = outputTopic.ReadKeyValuesToMap();
                Assert.AreEqual(2, mapRecords.Count);
                Assert.AreEqual("VALUE1", mapRecords["key1"]);
                Assert.AreEqual("VALUE2", mapRecords["key2"]);
            }
        }

        [Test]
        public void TransformerValuesAPILambda()
        {
            var builder = new StreamBuilder();
            
            builder.Stream<string, string>("topic")
                .TransformValues(TransformerBuilder
                    .New<string, string, string, string>()
                    .Transformer((record) => Record<string, string>.Create(record.Value.ToUpper()))
                    .Build())
                .To("topic-output");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-transformer-api";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("key1", "value1");
                inputTopic.PipeInput("key2", "value2");

                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                var mapRecords = outputTopic.ReadKeyValuesToMap();
                Assert.AreEqual(2, mapRecords.Count);
                Assert.AreEqual("VALUE1", mapRecords["key1"]);
                Assert.AreEqual("VALUE2", mapRecords["key2"]);
            }
        }

        [Test]
        public void TransformerWithRepartitionBefore()
        {
            var builder = new StreamBuilder();
            
            builder.Stream<string, string>("topic")
                .SelectKey((k,v) => v)
                .GroupByKey()
                .Count(InMemory.As<string, long>())
                .ToStream()
                .Transform(TransformerBuilder
                    .New<string, long, string, string>()
                    .Transformer((record) => Record<string, string>.Create(record.Key, record.Value.ToString()))
                    .Build())
                .To("topic-output");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-transformer-api";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("key1", "value1");
                inputTopic.PipeInput("key2", "value2");

                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                var mapRecords = outputTopic.ReadKeyValuesToMap();
                Assert.AreEqual(2, mapRecords.Count);
                Assert.AreEqual("1", mapRecords["value1"]);
                Assert.AreEqual("1", mapRecords["value2"]);
            } 
        }
        
        [Test]
        public void TransformerWithRepartitionAfter()
        {
            var builder = new StreamBuilder();
            
            builder.Stream<string, string>("topic")
                .Transform(TransformerBuilder
                    .New<string, string, string, string>()
                    .Transformer((record) => Record<string, string>.Create(record.Value, record.Value))
                    .Build())
                .GroupByKey()
                .Count(InMemory.As<string, long>())
                .ToStream()
                .MapValues((v) => v.ToString())
                .To("topic-output");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-transformer-api";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("key1", "value1");
                inputTopic.PipeInput("key2", "value2");

                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                var mapRecords = outputTopic.ReadKeyValuesToMap();
                Assert.AreEqual(2, mapRecords.Count);
                Assert.AreEqual("1", mapRecords["value1"]);
                Assert.AreEqual("1", mapRecords["value2"]);
            } 
        }
        
        [Test]
        public void TransformerValuesWithUpdateHeaders()
        {
            var builder = new StreamBuilder();
            
            builder.Stream<string, string>("topic")
                .TransformValues(TransformerBuilder
                    .New<string, string, string, string>()
                    .Transformer((record) =>
                    {
                        var headers = new Headers();
                        headers.Add("key1", Encoding.UTF8.GetBytes("value1"));
                        return Record<string, string>.Create(record.Value.ToUpper(), headers);
                    })
                    .Build())
                .To("topic-output");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-transformer-api";

            Topology t = builder.Build();

            using var driver = new TopologyTestDriver(t, config);
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("key1", "value1");

                var outputTopic = driver.CreateOuputTopic<string, string>("topic-output");
                var record = outputTopic.ReadKeyValue();
                Assert.AreEqual(1, record.Message.Headers.Count);
                Assert.AreEqual(
                    Encoding.UTF8.GetBytes("value1"),
                    record.Message.Headers.GetLastBytes("key1"));
            }
        }

    }
}