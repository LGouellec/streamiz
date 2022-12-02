using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Table;
using System.Linq;
using System.Security.Permissions;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Mock;
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
        private static readonly JsonSerializerOptions _jsonSerializerOptions = new();

        public class Tag
        {
            public string Field1 { get; set; }
            public string Field2 { get; set; }
            public string Field3 { get; set; }
        }

        public class Metadata
        {
            public Dictionary<string, string> Data { get; set; }
        }

        public class TagInfo
        {
            public Tag Tag { get; set; }
            public Metadata MetaData { get; set; }
        }
        
        public static async Task Main(string[] args)
        {
            string KeyMapping(string key, Tag tag)
            {
                return key;
            }
            
            string ValueJoiner(Tag currentValue, string metaValue)
            {
                var tag = new TagInfo
                {
                    Tag = currentValue,
                    MetaData = metaValue == null
                        ? null
                        : JsonSerializer.Deserialize<Metadata>(metaValue, _jsonSerializerOptions)
                };

                return JsonSerializer.Serialize(tag, _jsonSerializerOptions);
            }
            
            // globalTopic.PipeInput("key1", "{\"Data\":{\"key1\":\"value1\",\"key2\":\"value2\"}}");
            // inputTopic.PipeInput("key1", new Tag() {Field1 = "tag1", Field2 = "tag2", Field3 = "tag3"});

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app-reproducer";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.CommitIntervalMs = 3000;
            config.StateDir = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            config.Logger = LoggerFactory.Create((b) =>
            {
                b.SetMinimumLevel(LogLevel.Debug);
                b.AddLog4Net();
            });
            
            StreamBuilder builder = new StreamBuilder();

            var globalKTable = builder.GlobalTable("meta", InMemory.As<string, string>("table-store"));
            var kStream = builder.Stream<string, Tag, StringSerDes, JsonSerDes<Tag>>("stream");

            var targetStream = kStream
                .LeftJoin(globalKTable, KeyMapping, ValueJoiner);

            targetStream.To<StringSerDes, StringSerDes>("target");

            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (_, _) => stream.Dispose();
            
            await stream.StartAsync();
        }
    }
}