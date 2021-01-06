using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using System;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// If you want an example with token source passed to startasync, see <see cref="ProgramToken"/> class.
    /// </summary>
    internal class Program
    {
        static async System.Threading.Tasks.Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.BootstrapServers = "localhost:9093";
            config.FollowMetadata = true;
            config.NumStreamThreads = 2;

            StreamBuilder builder = new StreamBuilder();
            IKStream<string, string> kStream = builder.Stream<string, string>("test");
            kStream.MapValues((v) =>
            {
                var headers = StreamizMetadata.GetCurrentHeadersMetadata();
                if(headers.Count > 0 )
                    Console.WriteLine("RANDOM : " + BitConverter.ToInt32(headers.GetLastBytes("random")));
                return v;
            });
            kStream.Print(Printed<string, string>.ToOut());

            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => stream.Dispose();

            await stream.StartAsync();
        }
    }
}
