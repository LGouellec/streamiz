using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class TopologyModelTests
    {
        [Test]
        public void SimpleTopologyToModel()
        {
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic", new StringSerDes(), new StringSerDes(), "source")
                .Filter((k, v, _) => v.Contains("test"), "filter")
                .To("topic-dest", "to");

            var description = builder.Build().Describe();
            var model = description.ToModel();

            Assert.AreEqual(1, model.SubTopologies.Count);
            Assert.AreEqual(0, model.GlobalStores.Count);
            var sub = model.SubTopologies[0];
            Assert.AreEqual(0, sub.Id);
            Assert.AreEqual(new List<string> { "topic" }, sub.SourceTopics);
            Assert.AreEqual(new List<string> { "topic-dest" }, sub.SinkTopics);
            Assert.AreEqual(3, sub.Nodes.Count);

            var source = sub.Nodes[0];
            Assert.AreEqual("source", source.Name);
            Assert.AreEqual(TopologyNodeType.Source, source.Type);
            Assert.AreEqual(new List<string> { "topic" }, source.Topics);
            Assert.AreEqual(new List<string> { "filter" }, source.Successors);
            Assert.IsEmpty(source.Predecessors);

            var filter = sub.Nodes[1];
            Assert.AreEqual("filter", filter.Name);
            Assert.AreEqual(TopologyNodeType.Processor, filter.Type);
            Assert.IsEmpty(filter.Stores);
            Assert.AreEqual(new List<string> { "source" }, filter.Predecessors);
            Assert.AreEqual(new List<string> { "to" }, filter.Successors);

            var sink = sub.Nodes[2];
            Assert.AreEqual("to", sink.Name);
            Assert.AreEqual(TopologyNodeType.Sink, sink.Type);
            Assert.AreEqual("topic-dest", sink.Topic);
            Assert.IsNull(sink.TopicNameExtractor);
            Assert.IsEmpty(sink.Successors);

            Assert.AreEqual(new List<string> { "topic" }, model.SourceTopics);
            Assert.AreEqual(new List<string> { "topic-dest" }, model.SinkTopics);
            Assert.IsEmpty(model.StateStores);
            Assert.AreEqual(description.ToString(), model.ToString());
        }

        [Test]
        public void TopologyToJson()
        {
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic", new StringSerDes(), new StringSerDes(), "source")
                .To("topic-dest", "to");

            var json = builder.Build().Describe().ToJson();
            var obj = JObject.Parse(json);

            var nodes = (JArray)obj["subTopologies"][0]["nodes"];
            Assert.AreEqual(2, nodes.Count);
            Assert.AreEqual("source", nodes[0]["name"].Value<string>());
            Assert.AreEqual("source", nodes[0]["type"].Value<string>());
            Assert.AreEqual("topic", nodes[0]["topics"][0].Value<string>());
            Assert.AreEqual("to", nodes[1]["name"].Value<string>());
            Assert.AreEqual("sink", nodes[1]["type"].Value<string>());
            Assert.AreEqual("topic-dest", nodes[1]["topic"].Value<string>());
            // null values are omitted
            Assert.IsNull(nodes[1]["topicNameExtractor"]);
            Assert.AreEqual("topic", obj["sourceTopics"][0].Value<string>());
            Assert.AreEqual("topic-dest", obj["sinkTopics"][0].Value<string>());
            Assert.AreEqual(0, ((JArray)obj["globalStores"]).Count);

            var compact = builder.Build().Describe().ToJson(false);
            Assert.IsFalse(compact.Contains("\n"));
        }

        [Test]
        public void GlobalStoreToModel()
        {
            var builder = new StreamBuilder();
            builder.GlobalTable("global-topic", InMemory.As<string, string>("global-store"));
            builder.Stream<string, string>("topic").To("topic-dest");

            var model = builder.Build().Describe().ToModel();

            Assert.AreEqual(1, model.SubTopologies.Count);
            Assert.AreEqual(1, model.GlobalStores.Count);
            var global = model.GlobalStores[0];
            Assert.AreEqual("global-topic", global.Topic);
            Assert.AreEqual("global-store", global.Store);
            Assert.AreEqual(TopologyNodeType.Source, global.Source.Type);
            Assert.AreEqual(TopologyNodeType.Processor, global.Processor.Type);
            Assert.AreEqual(new List<string> { "global-store" }, global.Processor.Stores);
            Assert.AreEqual(new List<string> { "global-topic", "topic" }, model.SourceTopics);
            Assert.AreEqual(new List<string> { "global-store" }, model.StateStores);
        }

        [Test]
        public void KafkaStreamDescribeTopologyResolvesInternalTopics()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-app",
                BootstrapServers = "127.0.0.1"
            };

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic")
                .GroupBy((k, v, _) => k.ToUpper())
                .Count(InMemory.As<string, long>("count-store"))
                .ToStream()
                .To("output");

            var topology = builder.Build();
            using var stream = new KafkaStream(topology, config, new SyncKafkaSupplier());

            var model = stream.DescribeTopology();

            Assert.AreEqual(2, model.SubTopologies.Count);
            Assert.AreEqual(new List<string> { "count-store" }, model.StateStores);
            Assert.AreEqual(new List<string> { "test-app-count-store-changelog" }, model.ChangelogTopics);
            Assert.AreEqual(1, model.RepartitionTopics.Count);
            Assert.IsTrue(model.RepartitionTopics[0].StartsWith("test-app-"));
            Assert.IsTrue(model.RepartitionTopics[0].EndsWith("-repartition"));

            var aggregateSub = model.SubTopologies.Single(s => s.StateStores.Contains("count-store"));
            Assert.AreEqual(new List<string> { "test-app-count-store-changelog" }, aggregateSub.ChangelogTopics);
            Assert.AreEqual(model.RepartitionTopics, aggregateSub.RepartitionTopics);

            Assert.AreEqual(topology.Describe().ToString(), model.ToString());

            var obj = JObject.Parse(model.ToJson());
            Assert.AreEqual("test-app-count-store-changelog", obj["changelogTopics"][0].Value<string>());
        }
    }
}
