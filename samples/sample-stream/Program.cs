using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Table;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Security.Permissions;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Prometheus;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes.CloudEvents;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;

namespace sample_stream
{
    /// <summary>
    /// Sample program with a passtrought stream, instanciate and dispose with CTRL+ C console event.
    /// </summary>
    internal class Program
    {
        private static ILogger logger = Streamiz.Kafka.Net.Crosscutting.Logger.GetLogger(typeof(Program));
        class MyTransformer : ITransformer<string, string, string, int>
        {
            private IKeyValueStore<string,int> store;

            public void Init(ProcessorContext<string, int> context)
            {
                store = (IKeyValueStore<string, int>)context.GetStateStore("store");
                context.Schedule(
                    TimeSpan.FromMinutes(1),
                    PunctuationType.PROCESSING_TIME,
                    (ts) =>
                    {
                        foreach(var item in store.All())
                            context.Forward(item.Key, item.Value);
                    });
            }

            public Record<string, int> Process(Record<string, string> record)
            {
                var oldState = store.Get(record.Key);
                store.Put(record.Key, oldState + 1 );
                return null;
            }

            public void Close()
            {
                
            }
        }
        public static async Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.CommitIntervalMs = 3000;
            config.StateDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            config.Logger = LoggerFactory.Create((b) =>
            {
                b.SetMinimumLevel(LogLevel.Information);
                b.AddLog4Net();
            });
            config.UsePrometheusReporter(9090);
            config.MetricsRecording = MetricsRecordingLevel.DEBUG;
            
            StreamBuilder builder = new StreamBuilder();
            
            builder.Stream<string, string>("inputs")
                .Peek((k, v) => logger.LogInformation("key={k} value={v}", k,v))
                .MapValuesAsync(async (record, _) => await Task.FromResult(record.Value.ToUpper()))
                .To("output");
            
            Topology t = builder.Build();
            KafkaStream stream1 = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (_, _) => stream1.Dispose();
            
            await stream1.StartAsync();
        }
    }
}