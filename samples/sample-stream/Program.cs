using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Threading.Tasks;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// If you want an example with token source passed to startasync, see <see cref="ProgramToken"/> class.
    /// </summary>
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.BootstrapServers = "localhost:19092";
            config.PollMs = 25;
            config.MaxPollRecords = 1000;

            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, string>("test")
                .Filter((k, v) => !string.IsNullOrEmpty(v))
                .To("test-output");

            Topology t = builder.Build();

            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => {
                stream.Dispose();
            };

            await stream.StartAsync();
        }
    }
}
