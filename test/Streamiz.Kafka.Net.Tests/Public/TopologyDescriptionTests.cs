using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class TopologyDescriptionTests
    {
        private class MyTimestampExtractor : ITimestampExtractor
        {
            public long Extract(ConsumeResult<object, object> record, long partitionTime)
            {
                return DateTime.Now.GetMilliseconds();
            }
        }

        private class MytopicExtractor : ITopicNameExtractor<string, string>
        {
            public string Extract(string key, string value, IRecordContext recordContext)
            {
                return "toto";
            }
        }

        [Test]
        public void TopologyDescriptionOneCompleteSubtopology()
        {
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic", new StringSerDes(), new StringSerDes(), "source")
                .Filter((k, v) => v.Contains("test"), "filter")
                .To("topic-dest", "to");

            var topology = builder.Build();
            var description = topology.Describe();

            Assert.AreEqual(1, description.SubTopologies.Count());
            Assert.AreEqual("topic", description.SubTopologies.ToArray()[0].Id);
            Assert.AreEqual(3, description.SubTopologies.ToArray()[0].Nodes.Count());

            var sourceNode = description.SubTopologies.ToArray()[0].Nodes.FirstOrDefault(n => n is ISourceNodeDescription) as ISourceNodeDescription;
            var filterNode = description.SubTopologies.ToArray()[0].Nodes.FirstOrDefault(n => n is IProcessorNodeDescription) as IProcessorNodeDescription;
            var sinkNode = description.SubTopologies.ToArray()[0].Nodes.FirstOrDefault(n => n is ISinkNodeDescription) as ISinkNodeDescription;

            Assert.AreEqual("source", sourceNode.Name);
            Assert.AreEqual(null, sourceNode.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "topic" }, sourceNode.Topics);
            Assert.AreEqual(1, sourceNode.Next.Count());
            Assert.AreEqual(0, sourceNode.Previous.Count());
            Assert.AreEqual(filterNode, sourceNode.Next.ToArray()[0]);

            Assert.AreEqual("filter", filterNode.Name);
            Assert.AreEqual(0, filterNode.Stores.Count());
            Assert.AreEqual(new List<string> { "topic" }, sourceNode.Topics);
            Assert.AreEqual(1, filterNode.Next.Count());
            Assert.AreEqual(1, filterNode.Previous.Count());
            Assert.AreEqual(sinkNode, filterNode.Next.ToArray()[0]);
            Assert.AreEqual(sourceNode, filterNode.Previous.ToArray()[0]);

            Assert.AreEqual("to", sinkNode.Name);
            Assert.AreEqual(null, sinkNode.TopicNameExtractorType);
            Assert.AreEqual("topic-dest", sinkNode.Topic);
            Assert.AreEqual(0, sinkNode.Next.Count());
            Assert.AreEqual(1, sinkNode.Previous.Count());
            Assert.AreEqual(filterNode, sinkNode.Previous.ToArray()[0]);
        }

        [Test]
        public void TopologyDescriptionOneCompleteSubtopology2()
        {
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic", new StringSerDes(), new StringSerDes(), "source", new MyTimestampExtractor())
                .Filter((k, v) => v.Contains("test"), "filter")
                .To(new MytopicExtractor(), "to");

            var topology = builder.Build();
            var description = topology.Describe();

            Assert.AreEqual(1, description.SubTopologies.Count());
            Assert.AreEqual("topic", description.SubTopologies.ToArray()[0].Id);
            Assert.AreEqual(3, description.SubTopologies.ToArray()[0].Nodes.Count());

            var sourceNode = description.SubTopologies.ToArray()[0].Nodes.FirstOrDefault(n => n is ISourceNodeDescription) as ISourceNodeDescription;
            var filterNode = description.SubTopologies.ToArray()[0].Nodes.FirstOrDefault(n => n is IProcessorNodeDescription) as IProcessorNodeDescription;
            var sinkNode = description.SubTopologies.ToArray()[0].Nodes.FirstOrDefault(n => n is ISinkNodeDescription) as ISinkNodeDescription;

            Assert.AreEqual("source", sourceNode.Name);
            Assert.AreEqual(typeof(MyTimestampExtractor), sourceNode.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "topic" }, sourceNode.Topics);
            Assert.AreEqual(1, sourceNode.Next.Count());
            Assert.AreEqual(0, sourceNode.Previous.Count());
            Assert.AreEqual(filterNode, sourceNode.Next.ToArray()[0]);

            Assert.AreEqual("filter", filterNode.Name);
            Assert.AreEqual(0, filterNode.Stores.Count());
            Assert.AreEqual(new List<string> { "topic" }, sourceNode.Topics);
            Assert.AreEqual(1, filterNode.Next.Count());
            Assert.AreEqual(1, filterNode.Previous.Count());
            Assert.AreEqual(sinkNode, filterNode.Next.ToArray()[0]);
            Assert.AreEqual(sourceNode, filterNode.Previous.ToArray()[0]);

            Assert.AreEqual("to", sinkNode.Name);
            Assert.AreEqual(typeof(MytopicExtractor), sinkNode.TopicNameExtractorType);
            Assert.AreEqual(null, sinkNode.Topic);
            Assert.AreEqual(0, sinkNode.Next.Count());
            Assert.AreEqual(1, sinkNode.Previous.Count());
            Assert.AreEqual(filterNode, sinkNode.Previous.ToArray()[0]);
        }


        [Test]
        public void TopologyDescriptioTwoSubtopology()
        {
            var builder = new StreamBuilder();
            var stream1 = builder.Stream<string, string>("topic");
            var stream2 = builder.Stream<string, string>("topic2");

            var topology = builder.Build();
            var description = topology.Describe();

            Assert.AreEqual(2, description.SubTopologies.Count());
            Assert.AreEqual("topic", description.SubTopologies.ToArray()[0].Id);
            Assert.AreEqual(1, description.SubTopologies.ToArray()[0].Nodes.Count());
            Assert.AreEqual("topic2", description.SubTopologies.ToArray()[1].Id);
            Assert.AreEqual(1, description.SubTopologies.ToArray()[1].Nodes.Count());

            var sourceNode1 = description.SubTopologies.ToArray()[0].Nodes.FirstOrDefault(n => n is ISourceNodeDescription) as ISourceNodeDescription;
            var sourceNode2 = description.SubTopologies.ToArray()[1].Nodes.FirstOrDefault(n => n is ISourceNodeDescription) as ISourceNodeDescription;

            Assert.IsTrue(sourceNode1.Name.StartsWith(KStream<int, int>.SOURCE_NAME));
            Assert.AreEqual(null, sourceNode1.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "topic" }, sourceNode1.Topics);
            Assert.AreEqual(0, sourceNode1.Next.Count());
            Assert.AreEqual(0, sourceNode1.Previous.Count());

            Assert.IsTrue(sourceNode2.Name.StartsWith(KStream<int, int>.SOURCE_NAME));
            Assert.AreEqual(null, sourceNode2.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "topic2" }, sourceNode2.Topics);
            Assert.AreEqual(0, sourceNode2.Next.Count());
            Assert.AreEqual(0, sourceNode2.Previous.Count());
        }

    }
}
