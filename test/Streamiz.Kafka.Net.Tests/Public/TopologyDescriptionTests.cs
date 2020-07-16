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
            Assert.AreEqual(0, description.SubTopologies.ToArray()[0].Id);
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
            Assert.AreEqual(0, description.SubTopologies.ToArray()[0].Id);
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
            Assert.AreEqual(0, description.SubTopologies.ToArray()[0].Id);
            Assert.AreEqual(1, description.SubTopologies.ToArray()[0].Nodes.Count());
            Assert.AreEqual(1, description.SubTopologies.ToArray()[1].Id);
            Assert.AreEqual(1, description.SubTopologies.ToArray()[1].Nodes.Count());

            var sourceNode1 = description.SubTopologies.ToArray()[0].Nodes.FirstOrDefault(n => n is ISourceNodeDescription) as ISourceNodeDescription;
            var sourceNode2 = description.SubTopologies.ToArray()[1].Nodes.FirstOrDefault(n => n is ISourceNodeDescription) as ISourceNodeDescription;

            Assert.IsTrue(sourceNode1.Name.StartsWith(KStream.SOURCE_NAME));
            Assert.AreEqual(null, sourceNode1.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "topic" }, sourceNode1.Topics);
            Assert.AreEqual(0, sourceNode1.Next.Count());
            Assert.AreEqual(0, sourceNode1.Previous.Count());

            Assert.IsTrue(sourceNode2.Name.StartsWith(KStream.SOURCE_NAME));
            Assert.AreEqual(null, sourceNode2.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "topic2" }, sourceNode2.Topics);
            Assert.AreEqual(0, sourceNode2.Next.Count());
            Assert.AreEqual(0, sourceNode2.Previous.Count());
        }

        [Test]
        public void TopologyDescriptionJoinTopology()
        {
            var builder = new StreamBuilder();
            var stream1 = builder.Stream("topic1", new StringSerDes(), new StringSerDes());
            var stream2 = builder.Stream("topic2", new StringSerDes(), new StringSerDes());

            stream1.Join<string, string, StringSerDes, StringSerDes>(stream2,
                (v, v2) => $"{v}-{v2}",
                JoinWindowOptions.Of(TimeSpan.FromMinutes(1)))
                .To("topic-o");

            var topology = builder.Build();
            var description = topology.Describe();

            // JAVA 
    //        Sub - topology: 0
    //Source: KSTREAM - SOURCE - 0000000000(topics: [test])
    //  -- > KSTREAM - WINDOWED - 0000000002
    //Source: KSTREAM - SOURCE - 0000000001(topics: [test2])
    //  -- > KSTREAM - WINDOWED - 0000000003
    //Processor: KSTREAM - WINDOWED - 0000000002(stores: [KSTREAM - JOINTHIS - 0000000004 - store])
    //  -- > KSTREAM - JOINTHIS - 0000000004
    //  < --KSTREAM - SOURCE - 0000000000
    //Processor: KSTREAM - WINDOWED - 0000000003(stores: [KSTREAM - JOINOTHER - 0000000005 - store])
    //  -- > KSTREAM - JOINOTHER - 0000000005
    //  < --KSTREAM - SOURCE - 0000000001
    //Processor: KSTREAM - JOINOTHER - 0000000005(stores: [KSTREAM - JOINTHIS - 0000000004 - store])
    //  -- > KSTREAM - MERGE - 0000000006
    //  < --KSTREAM - WINDOWED - 0000000003
    //Processor: KSTREAM - JOINTHIS - 0000000004(stores: [KSTREAM - JOINOTHER - 0000000005 - store])
    //  -- > KSTREAM - MERGE - 0000000006
    //  < --KSTREAM - WINDOWED - 0000000002
    //Processor: KSTREAM - MERGE - 0000000006(stores: [])
    //  -- > KSTREAM - SINK - 0000000007
    //  < --KSTREAM - JOINTHIS - 0000000004, KSTREAM - JOINOTHER - 0000000005
    //Sink: KSTREAM - SINK - 0000000007(topic: output - join)
    //  < --KSTREAM - MERGE - 0000000006



      // C# 
    //        Sub - topology: 0

    //Source: KSTREAM - SOURCE--0000000001(topics: topic1)
    //         -- > KSTREAM - JOINTHIS--0000000005 - store

    //Source: KSTREAM - SOURCE--0000000002(topics: topic2)
    //         -- > KSTREAM - JOINOTHER--0000000006 - store

    //Processor: KSTREAM - JOINTHIS--0000000005 - store(stores: KSTREAM - JOINTHIS--0000000005 - store)
    //         -- > KSTREAM - JOINTHIS--0000000005
    //            < --KSTREAM - SOURCE--0000000001

    //Processor: KSTREAM - JOINOTHER--0000000006 - store(stores: KSTREAM - JOINOTHER--0000000006 - store)
    //         -- > KSTREAM - JOINOTHER--0000000006
    //            < --KSTREAM - SOURCE--0000000002

    //Processor: KSTREAM - JOINTHIS--0000000005(stores: KSTREAM - JOINOTHER--0000000006 - store)
    //         -- > KSTREAM - MERGE--0000000007
    //            < --KSTREAM - JOINTHIS--0000000005 - store

    //Processor: KSTREAM - JOINOTHER--0000000006(stores: KSTREAM - JOINTHIS--0000000005 - store)
    //         -- > KSTREAM - MERGE--0000000007
    //            < --KSTREAM - JOINOTHER--0000000006 - store

    //Processor: KSTREAM - MERGE--0000000007(stores: )
    //         -- > KSTREAM - SINK--0000000008
    //            < --KSTREAM - JOINTHIS--0000000005, KSTREAM - JOINOTHER--0000000006

    //Sink: KSTREAM - SINK--0000000008(topic: topic - o)
    //         < --KSTREAM - MERGE--0000000007


            Assert.AreEqual(1, description.SubTopologies.Count());
            Assert.AreEqual(0, description.SubTopologies.ToArray()[0].Id);
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

    }
}
