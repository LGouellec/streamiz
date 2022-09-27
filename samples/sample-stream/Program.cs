using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Table;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// </summary>
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = $"",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                StartTaskDelayMs = (long)TimeSpan.FromDays(1).TotalMilliseconds
            };

            var builder = JoinStreamWithTable();

            var t = builder.Build();
            var stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => { stream.Dispose(); };

            await stream.StartAsync();

            Console.WriteLine("Finished");
        }

        private static StreamBuilder JoinStreamWithTable()
        {
            var builder = new StreamBuilder();

            var table = builder.GlobalTable("JoinSample-1",
                InMemory<string, string>.As("table-store"));

            var stream = builder.Stream<string, string>("JoinSample-2");

            var joinedStream =
                stream.LeftJoin(table, (k, v) => k, (v1, v2) => $"{v1}joinedWith{v2}");

            joinedStream.To("JoinedEvent");

            return builder;
        }
    }
}