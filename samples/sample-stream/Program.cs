using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Table;
using System.Linq;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Stream;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// </summary>
    internal class Program
    {
        public static async Task Main(string[] args)
        {

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app-reproducer";
           config.BootstrapServers = "XXXXXXXXXXX";
           // config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.SaslMechanism = SaslMechanism.Plain;
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslUsername = "$ConnectionString";
            config.SaslPassword = "Endpoint=sb://XXXXXXX/;SharedAccessKeyName=XXXXXX;SharedAccessKey=XXXXX";
            config.SslCaLocation = "./cacert.pem";
            //config.Debug = "security,broker,protocol";
            config.Logger = LoggerFactory.Create((b) =>
            {
                b.SetMinimumLevel(LogLevel.Debug);
                b.AddLog4Net();
            });

            StreamBuilder builder = new StreamBuilder();
            
            builder.Stream<string, string>("input")
                .Print(Printed<string, string>.ToOut());
            
            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (_, _) => stream.Dispose();
            
            await stream.StartAsync();
        }
    }
}