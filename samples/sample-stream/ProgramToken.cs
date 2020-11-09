using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// To dispose stream, you have just to cancel token source. 
    /// Can be useful if you use cancel token from a ASP .NET Web API for example.
    /// </summary>
    public class ProgramToken
    {
        private static async Task MainToken(string[] args)
        {
            CancellationTokenSource source = new CancellationTokenSource();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-token-app";
            config.BootstrapServers = "localhost:29092";
            config.PollMs = 100;
            config.MaxPollRecords = 500;
            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, string>("test")
                    .To("test-output");

            Topology t = builder.Build();

            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) =>
            {
                source.Cancel();
            };

            await stream.StartAsync(source.Token);
        }
    }
}
