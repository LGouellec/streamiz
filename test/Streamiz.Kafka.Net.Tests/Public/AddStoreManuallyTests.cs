using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
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
            private IKeyValueStore<string,string> stateStore;

            public void Init(ProcessorContext<string, string> context)
            {
                stateStore = (IKeyValueStore<string, string>)context.GetStateStore("output-store");
            }

            public Record<string, string> Process(Record<string, string> record)
            {
                // De-duplication process
                if (stateStore.Get(record.Key) == null)
                {
                    stateStore.Put(record.Key, record.Value);
                    return record;
                }

                return null;
            }

            public void Close()
            {
                
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
                var outputTopic = driver.CreateOuputTopic<string, string>("output-topic");

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
    }
}