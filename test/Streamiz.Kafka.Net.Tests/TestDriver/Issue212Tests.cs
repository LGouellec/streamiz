using System.Collections.Generic;
using System.Text.Json;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.TestDriver
{
    public class Issue212Tests
    {
        private static readonly JsonSerializerOptions _jsonSerializerOptions = new();

        private class Tag
        {
            public string Field1 { get; set; }
            public string Field2 { get; set; }
            public string Field3 { get; set; }
        }

        private class Metadata
        {
            public Dictionary<string, string> Data { get; set; }
        }

        private class TagInfo
        {
            public Tag Tag { get; set; }
            public Metadata MetaData { get; set; }
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

        string KeyMapping(string key, Tag tag)
        {
            return key;
        }

        [Test]
        public void Reproducer()
        {
            var streamConfig = new StreamConfig<StringSerDes, StringSerDes>();
            streamConfig.ApplicationId = "test-reproducer-issue212";

            var builder = new StreamBuilder();

            var globalKTable = builder.GlobalTable("meta", InMemory.As<string, string>("table-store"));
            var kStream = builder.Stream<string, Tag, StringSerDes, JsonSerDes<Tag>>("stream");

            var targetStream = kStream
                .LeftJoin(globalKTable, KeyMapping, ValueJoiner);

            targetStream.To<StringSerDes, StringSerDes>("target");

            var topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, streamConfig))
            {
                var globalTopic = driver.CreateInputTopic<string, string>("meta");
                var inputTopic = driver.CreateInputTopic<string, Tag, StringSerDes, JsonSerDes<Tag>>("stream");
                var result = driver.CreateOutputTopic<string, string>("target");

                globalTopic.PipeInput("key1", "{\"Data\":{\"key1\":\"value1\",\"key2\":\"value2\"}}");
                inputTopic.PipeInput("key1", new Tag() {Field1 = "tag1", Field2 = "tag2", Field3 = "tag3"});

                var records = result.ReadValueList();
            }
        }
    }
}