using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.OpenTelemetry;

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
            config.ApplicationId = "app-count-word";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.StateDir = Path.Combine("/tmp/state");
            config.CommitIntervalMs = 5000;
            config.Logger = LoggerFactory.Create(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Debug);
                builder.AddLog4Net();
            });
            config.MetricsRecording = MetricsRecordingLevel.DEBUG;
            config.UseOpenTelemetryReporter();

            StreamBuilder builder = new StreamBuilder();
            builder.Stream<string, string>("topic1").To("topic2");
            
            builder.Stream<string, string>("words")
                .FlatMapValues((k, v) => v.Split(" "))
                .SelectKey((k, v) => v)
                .GroupByKey()
                .Count(
                    RocksDb<string, long>
                        .As("count-store")
                        .WithKeySerdes(new StringSerDes())
                        .WithValueSerdes(new Int64SerDes()))
                .ToStream()
                .Print(Printed<string, long>.ToOut());
            
            var topo = builder.Build();
            
            KafkaStream stream = new KafkaStream(topo, config);

            Console.CancelKeyPress += (o,e) => stream.Dispose();
            
            await stream.StartAsync();
        }
    }
    
    public class ABC : AbstractSerDes<ABC>
    {
        public string Name { get; set; }
        public int Data { get; set; }
        
        public override ABC Deserialize(byte[] data, SerializationContext context)
        {
            var bytesAsString = Encoding.UTF8.GetString(data);
            return JsonConvert.DeserializeObject<ABC>(bytesAsString);
        }

        public override byte[] Serialize(ABC data, SerializationContext context)
        {
            var a = JsonConvert.SerializeObject(data);
            return Encoding.UTF8.GetBytes(a);
        }
    }
}