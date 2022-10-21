using System.Collections.Generic;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamProcessorAPITests
    {
        private class MyProcessor : IProcessor<string, string>
        {
            private readonly List<(string, string)> data;

            public MyProcessor(List<(string, string)> data)
            {
                this.data = data;
            }
            
            public void Init(ProcessorContext context)
            {
                
            }

            public void Process(Record<string, string> record)
            {
                data.Add((record.Key, record.Value));
            }

            public void Close()
            {
                
            }
        }

        private class MyStatefullProcessor : IProcessor<string, string>
        {
            private IKeyValueStore<string, string> store;
            
            public void Init(ProcessorContext context)
            {
                store = (IKeyValueStore<string, string>)context.GetStateStore("my-store");
            }

            public void Process(Record<string, string> record)
            {
                store.Put(record.Key, record.Value);
            }

            public void Close()
            {
                
            }
        }

        [Test]
        public void ProcessorAPILambda()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            
            builder.Stream<string, string>("topic")
                .Process(ProcessorBuilder
                    .New<string, string>()
                    .Processor((record) =>
                    {
                        data.Add(KeyValuePair.Create(record.Key, record.Value));
                    })
                    .Build());

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-processor-api";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("key1", "value1");
                inputTopic.PipeInput("key2", "value2");
                
                Assert.AreEqual(2, data.Count);
                Assert.AreEqual("key1", data[0].Key);
                Assert.AreEqual("key2", data[1].Key);
            }
        }
        
        [Test]
        public void ProcessorAPIProcessor()
        {
            var builder = new StreamBuilder();
            var data = new List<(string, string)>();
            
            builder.Stream<string, string>("topic")
                .Process(ProcessorBuilder
                    .New<string, string>()
                    .Processor(new MyProcessor(data))
                    .Build());

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-processor-api";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("key1", "value1");
                inputTopic.PipeInput("key2", "value2");
                
                Assert.AreEqual(2, data.Count);
                Assert.AreEqual("key1", data[0].Item1);
                Assert.AreEqual("key2", data[1].Item1);
            }
        }

        [Test]
        public void ProcessorAPIStatefull()
        {
            var builder = new StreamBuilder();
            
            builder.Stream<string, string>("topic")
                .Process(ProcessorBuilder
                    .New<string, string>()
                    .Processor(new MyStatefullProcessor())
                    .StateStore(State.Stores.KeyValueStoreBuilder(
                            State.Stores.InMemoryKeyValueStore("my-store"),
                            new StringSerDes(),
                            new StringSerDes()))
                    .Build());

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-processor-api";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("key1", "value1");
                inputTopic.PipeInput("key2", "value2");

                var store = driver.GetKeyValueStore<string, string>("my-store");
                Assert.AreEqual(2, store.ApproximateNumEntries());
                Assert.AreEqual("value1", store.Get("key1"));
                Assert.AreEqual("value2", store.Get("key2"));
            }
        }

    }
}