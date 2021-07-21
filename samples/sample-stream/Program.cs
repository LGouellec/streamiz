using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// If you want an example with token source passed to startasync, see <see cref="ProgramToken"/> class.
    /// </summary>
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app-123";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;

            StreamBuilder builder = new StreamBuilder();

            builder.GlobalTable("dima-test", InMemory<string, string>.As("dima-test-store"));

            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);
            await stream.StartAsync();

            await Task.Delay(2000);

            stream.Dispose();
        }
    }
}