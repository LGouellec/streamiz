using System.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamContextForwarder
    {
        private class TransformerForwarder : ITransformer<string, string, string, string>
        {
            private IKeyValueStore<string,string> store;
            private ProcessorContext<string, string> context;
            
            public void Init(ProcessorContext<string, string> context)
            {
                store = (IKeyValueStore<string, string>)context.GetStateStore("my-store");
                this.context = context;
            }

            public Record<string, string> Process(Record<string, string> record)
            {
                var lastValue = store.Get(record.Key);
                if (string.IsNullOrEmpty(lastValue) || !lastValue.Equals(record.Value))
                {
                    store.Put(record.Key, record.Value);
                    context.Forward(record.Key, record.Value);
                }
                
                return null;
            }

            public void Close()
            {
                
            }
        }
        
        [Test]
        public void TestForwarder()
        {
            var builder = new StreamBuilder();
            
            var stream = builder.Stream<string, string>("topic")
                .Transform(TransformerBuilder
                    .New<string, string, string, string>()
                    .Transformer<TransformerForwarder>()
                    .StateStore(State.Stores.KeyValueStoreBuilder(
                        State.Stores.InMemoryKeyValueStore("my-store"),
                        new StringSerDes(),
                        new StringSerDes()))
                    .Build());
            
            stream.To("topic-output1");
            stream.To("topic-output2");

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-forwarder-api";

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInput("key", "value1");
                inputTopic.PipeInput("key", "value2");
                inputTopic.PipeInput("key", "value2");
                
                var outputTopic1 = driver.CreateOuputTopic<string, string>("topic-output1");
                var outputTopic2 = driver.CreateOuputTopic<string, string>("topic-output2");
                
                var mapRecords1 = outputTopic1.ReadKeyValueList();
                var mapRecords2 = outputTopic2.ReadKeyValueList();
                
                Assert.AreEqual(2, mapRecords1.Count());
                Assert.AreEqual(2, mapRecords2.Count());
            }
        }

    }
}