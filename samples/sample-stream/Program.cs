using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Table;
using System.Linq;
using System.Security.Permissions;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// </summary>
    internal class Program
    {
        private class MyTransformer : ITransformer<string, string, string, string>
        {
            private IKeyValueStore<string,string> store;

            public void Init(ProcessorContext context)
            {
                store = (IKeyValueStore<string, string>)context.GetStateStore("my-store");
            }

            public Record<string, string> Process(Record<string, string> record)
            {
                if (store.Get(record.Key) != null)
                    store.Delete(record.Key);
                
                store.Put(record.Key, record.Value);
                
                return Record<string,string>.Create(record.Key, record.Value);
            }

            public void Close()
            {
                
            }
        }
        public static async Task Main(string[] args)
        {

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app-reproducer";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.Logger = LoggerFactory.Create((b) =>
            {
                b.SetMinimumLevel(LogLevel.Debug);
                b.AddLog4Net();
            });

            StreamBuilder builder = new StreamBuilder();

            builder.Stream<string, string>("input")
                .Transform(
                    TransformerBuilder.New<string, string, string, string>()
                        .Transformer(new MyTransformer())
                        .StateStore(Stores.KeyValueStoreBuilder(
                                Stores.InMemoryKeyValueStore("my-store"),
                                new StringSerDes(),
                                new StringSerDes()))
                        .Build());
            
            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (_, _) => stream.Dispose();
            
            await stream.StartAsync();
        }
    }
}