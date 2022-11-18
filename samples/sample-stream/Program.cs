using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Table;
using System.Linq;
using System.Security.Permissions;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Processors;
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

        private class MyTimestampExtractor : ITimestampExtractor
        {
            public long Extract(ConsumeResult<object, object> record, long partitionTime)
            {
                // remove milliseconds
                long ts = record.Message.Timestamp.UnixTimestampMs;
                return 1000 * (ts / 1000);
            }
        }


        public static async Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app-reproducer";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.CommitIntervalMs = 3000;
            config.Logger = LoggerFactory.Create((b) =>
            {
                b.SetMinimumLevel(LogLevel.Information);
                b.AddLog4Net();
            });
            
            config.DefaultTimestampExtractor = new MyTimestampExtractor();

            StreamBuilder builder = new StreamBuilder();

            builder.Stream<string, string>("input")
                .Filter((k,v) => v != null)
                .SelectKey((k,v) => v.Substring(0, 3))
                .Transform(
                    TransformerBuilder.New<string, string, string, string>()
                        .Transformer<MyTransformer>()
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