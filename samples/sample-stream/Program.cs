using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Stream;
using System.Collections.Generic;

namespace sample_stream
{
    public static class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"test-app",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var t = BuildTopology();
            var stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (_,_) => {
                stream.Dispose();
            };

            await stream.StartAsync();
        }
        
        private static Topology BuildTopology()
        {
            var builder = new StreamBuilder();
            
            var globalTable = builder
                .GlobalTable("Input2", new Int64SerDes(), new StringSerDes());
            
            var inputStream = builder
                .Stream<string, string>("Input", new StringSerDes(), new StringSerDes())
                .Map((k, v) => KeyValuePair.Create(1L, v.Length))
                .LeftJoin<long, string, int>(globalTable, (k1, k2) => k1, (sO, g) =>
                {
                    return sO;
                })
                .GroupByKey<Int64SerDes, Int32SerDes>()
                .Aggregate(() => new List<int>(), (l, i, arg3) =>
                {
                    arg3.Add(i);
                    return arg3;
                });
          
          return builder.Build();
        }
    }
}