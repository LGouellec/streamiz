﻿using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka.Admin;
using Streamiz.Kafka.Net.Processors.Internal;

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
                .Filter((k, v, _) => v.Contains("test"), "filter")
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
                .Filter((k, v, _) => v.Contains("test"), "filter")
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
        public void TopologyDescriptionJoinCompareWithJavaToString()
        {
            #region Expected
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("Topologies:");
            stringBuilder.AppendLine("   Sub-topology: 0");
            stringBuilder.AppendLine("    Source: KSTREAM-SOURCE-0000000000 (topics: [test])");
            stringBuilder.AppendLine("      --> KSTREAM-WINDOWED-0000000002");
            stringBuilder.AppendLine("    Source: KSTREAM-SOURCE-0000000001 (topics: [test2])");
            stringBuilder.AppendLine("      --> KSTREAM-WINDOWED-0000000003");
            stringBuilder.AppendLine("    Processor: KSTREAM-WINDOWED-0000000002 (stores: [KSTREAM-JOINTHIS-0000000004-store])");
            stringBuilder.AppendLine("      --> KSTREAM-JOINTHIS-0000000004");
            stringBuilder.AppendLine("      <-- KSTREAM-SOURCE-0000000000");
            stringBuilder.AppendLine("    Processor: KSTREAM-WINDOWED-0000000003 (stores: [KSTREAM-JOINOTHER-0000000005-store])");
            stringBuilder.AppendLine("      --> KSTREAM-JOINOTHER-0000000005");
            stringBuilder.AppendLine("      <-- KSTREAM-SOURCE-0000000001");
            stringBuilder.AppendLine("    Processor: KSTREAM-JOINTHIS-0000000004 (stores: [KSTREAM-JOINOTHER-0000000005-store])");
            stringBuilder.AppendLine("      --> KSTREAM-MERGE-0000000006");
            stringBuilder.AppendLine("      <-- KSTREAM-WINDOWED-0000000002");
            stringBuilder.AppendLine("    Processor: KSTREAM-JOINOTHER-0000000005 (stores: [KSTREAM-JOINTHIS-0000000004-store])");
            stringBuilder.AppendLine("      --> KSTREAM-MERGE-0000000006");
            stringBuilder.AppendLine("      <-- KSTREAM-WINDOWED-0000000003");
            stringBuilder.AppendLine("    Processor: KSTREAM-MERGE-0000000006 (stores: [])");
            stringBuilder.AppendLine("      --> KSTREAM-SINK-0000000007");
            stringBuilder.AppendLine("      <-- KSTREAM-JOINTHIS-0000000004, KSTREAM-JOINOTHER-0000000005");
            stringBuilder.AppendLine("    Sink: KSTREAM-SINK-0000000007 (topic: output-join)");
            stringBuilder.AppendLine("      <-- KSTREAM-MERGE-0000000006");
            #endregion
            // COMPARE TOSTRING() WITH JAVA TOSTRING() DESCRIBE TOPO
            var builder = new StreamBuilder();
            var stream1 = builder.Stream("test", new StringSerDes(), new StringSerDes());
            var stream2 = builder.Stream("test2", new StringSerDes(), new StringSerDes());

            stream1.Join<string, string, StringSerDes, StringSerDes>(stream2,
                (v, v2) => $"{v}-{v2}",
                JoinWindowOptions.Of(TimeSpan.FromMinutes(1)))
                .To("output-join");

            var topology = builder.Build();
            var description = topology.Describe();

            Assert.AreEqual(stringBuilder.ToString(), description.ToString());
        }

        [Test]
        public void TopologyDescriptionGlobalCompareWithJavaToString()
        {
            #region Expected
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("Topologies:");
            stringBuilder.AppendLine("   Sub-topology: 0 for global store (will not generate tasks)");
            stringBuilder.AppendLine("    Source: KSTREAM-SOURCE-0000000001 (topics: [test])");
            stringBuilder.AppendLine("      --> KTABLE-SOURCE-0000000002");
            stringBuilder.AppendLine("    Processor: KTABLE-SOURCE-0000000002 (stores: [test-STATE-STORE-0000000000])");
            stringBuilder.AppendLine("      --> none");
            stringBuilder.AppendLine("      <-- KSTREAM-SOURCE-0000000001");
            stringBuilder.AppendLine("   Sub-topology: 1");
            stringBuilder.AppendLine("    Source: KSTREAM-SOURCE-0000000003 (topics: [test2])");
            stringBuilder.AppendLine("      --> KSTREAM-JOIN-0000000004");
            stringBuilder.AppendLine("    Processor: KSTREAM-JOIN-0000000004 (stores: [])");
            stringBuilder.AppendLine("      --> KSTREAM-SINK-0000000005");
            stringBuilder.AppendLine("      <-- KSTREAM-SOURCE-0000000003");
            stringBuilder.AppendLine("    Sink: KSTREAM-SINK-0000000005 (topic: output-join)");
            stringBuilder.AppendLine("      <-- KSTREAM-JOIN-0000000004");
            #endregion
            // COMPARE TOSTRING() WITH JAVA TOSTRING() DESCRIBE TOPO
            var builder = new StreamBuilder();
            var table = builder.GlobalTable("test", new StringSerDes(), new StringSerDes());
            var stream2 = builder.Stream("test2", new StringSerDes(), new StringSerDes());

            stream2.Join(table, (k, k1) => k, (v, v2) => $"{v}-{v2}")
                    .To("output-join");

            var topology = builder.Build();
            var description = topology.Describe();

            Assert.AreEqual(stringBuilder.ToString(), description.ToString());
        }

        [Test]
        public void TopologyDescriptionRepartitionTopology()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic")
                .Map((k, v, _) => KeyValuePair.Create(k.ToUpper(), v))
                .ToTable()
                .ToStream()
                .To("return");
            
            var topology = builder.Build();
            var description = topology.Describe();

            Assert.AreEqual(2, description.SubTopologies.Count());
            Assert.AreEqual(0, description.SubTopologies.ToArray()[0].Id);
            Assert.AreEqual(4, description.SubTopologies.ToArray()[0].Nodes.Count());
            Assert.AreEqual(1, description.SubTopologies.ToArray()[1].Id);
            Assert.AreEqual(4, description.SubTopologies.ToArray()[1].Nodes.Count());

            var sourceNode1 = description.SubTopologies.ToArray()[0].Nodes.FirstOrDefault(n => n is ISourceNodeDescription) as ISourceNodeDescription;
            var sourceNode2 = description.SubTopologies.ToArray()[1].Nodes.FirstOrDefault(n => n is ISourceNodeDescription) as ISourceNodeDescription;

            Assert.IsTrue(sourceNode1.Name.StartsWith(KStream.SOURCE_NAME));
            Assert.AreEqual(null, sourceNode1.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "topic" }, sourceNode1.Topics);

            Assert.IsTrue(sourceNode2.Name.StartsWith(KStream.SOURCE_NAME));
            Assert.AreEqual(typeof(FailOnInvalidTimestamp), sourceNode2.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "KSTREAM-TOTABLE-0000000002-repartition" }, sourceNode2.Topics);
        }

        [Test]
        public void TopologyDescriptionRepartitionJoinStreamStreamTopology()
        {
            var builder = new StreamBuilder();
            var streamJoin = builder.Stream<String, String>("topic-to-join");
            
            builder.Stream<string, string>("topic")
                .Map((k, v, _) => KeyValuePair.Create(k.ToUpper(), v))
                .Join(streamJoin,
                    (s, s1) => $"{s}-{s1}",
                    JoinWindowOptions.Of(TimeSpan.FromSeconds(1)))
                .To("return");
            
            var topology = builder.Build();
            var description = topology.Describe();

            Assert.AreEqual(2, description.SubTopologies.Count());
            Assert.AreEqual(0, description.SubTopologies.ToArray()[0].Id);
            Assert.AreEqual(8, description.SubTopologies.ToArray()[0].Nodes.Count());
            Assert.AreEqual(1, description.SubTopologies.ToArray()[1].Id);
            Assert.AreEqual(4, description.SubTopologies.ToArray()[1].Nodes.Count());

            var sourceNode1 = description.SubTopologies.ToArray()[0].Nodes.ToArray()[0] as ISourceNodeDescription;
            var sourceNode1bis = description.SubTopologies.ToArray()[0].Nodes.ToArray()[1] as ISourceNodeDescription;
            var sourceNode2 = description.SubTopologies.ToArray()[1].Nodes.ToArray()[0] as ISourceNodeDescription;

            Assert.IsNotNull(sourceNode1);
            Assert.IsTrue(sourceNode1.Name.StartsWith(KStream.SOURCE_NAME));
            Assert.AreEqual(null, sourceNode1.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "topic-to-join" }, sourceNode1.Topics);

            Assert.IsNotNull(sourceNode1bis);
            Assert.IsTrue(sourceNode1bis.Name.StartsWith(KStream.SOURCE_NAME));
            Assert.AreEqual(typeof(FailOnInvalidTimestamp), sourceNode1bis.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "KSTREAM-MAP-0000000002-repartition" }, sourceNode1bis.Topics);
            
            Assert.IsTrue(sourceNode2.Name.StartsWith(KStream.SOURCE_NAME));
            Assert.AreEqual(null, sourceNode2.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "topic" }, sourceNode2.Topics);
        }

        [Test]
        public void TopologyDescriptionRepartitionGroupByAggTopology()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic")
                .GroupBy((k,v, _)=> KeyValuePair.Create(k.ToUpper(),v))
                .Count()
                .ToStream()
                .To("return");
            
            var topology = builder.Build();
            var description = topology.Describe();

            Assert.AreEqual(2, description.SubTopologies.Count());
            Assert.AreEqual(0, description.SubTopologies.ToArray()[0].Id);
            Assert.AreEqual(4, description.SubTopologies.ToArray()[0].Nodes.Count());
            Assert.AreEqual(1, description.SubTopologies.ToArray()[1].Id);
            Assert.AreEqual(4, description.SubTopologies.ToArray()[1].Nodes.Count());

            var sourceNode1 = description.SubTopologies.ToArray()[0].Nodes.FirstOrDefault(n => n is ISourceNodeDescription) as ISourceNodeDescription;
            var sourceNode2 = description.SubTopologies.ToArray()[1].Nodes.FirstOrDefault(n => n is ISourceNodeDescription) as ISourceNodeDescription;

            Assert.IsTrue(sourceNode1.Name.StartsWith(KStream.SOURCE_NAME));
            Assert.AreEqual(null, sourceNode1.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "topic" }, sourceNode1.Topics);

            Assert.IsTrue(sourceNode2.Name.StartsWith(KStream.SOURCE_NAME));
            Assert.AreEqual(typeof(FailOnInvalidTimestamp), sourceNode2.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition" }, sourceNode2.Topics);
        }

        [Test]
        public void TopologyDescriptionRepartitionTableGroupByAggTopology()
        {
            var builder = new StreamBuilder();
            builder
                .Table<string, string>("topic")
                .GroupBy((k,v, _)=> KeyValuePair.Create(k.ToUpper(),v))
                .Count()
                .ToStream()
                .To("return");
            
            var topology = builder.Build();
            var description = topology.Describe();

            Assert.AreEqual(2, description.SubTopologies.Count());
            Assert.AreEqual(0, description.SubTopologies.ToArray()[0].Id);
            Assert.AreEqual(4, description.SubTopologies.ToArray()[0].Nodes.Count());
            Assert.AreEqual(1, description.SubTopologies.ToArray()[1].Id);
            Assert.AreEqual(4, description.SubTopologies.ToArray()[1].Nodes.Count());

            var sourceNode1 = description.SubTopologies.ToArray()[0].Nodes.FirstOrDefault(n => n is ISourceNodeDescription) as ISourceNodeDescription;
            var sourceNode2 = description.SubTopologies.ToArray()[1].Nodes.FirstOrDefault(n => n is ISourceNodeDescription) as ISourceNodeDescription;

            Assert.IsTrue(sourceNode1.Name.StartsWith(KStream.SOURCE_NAME));
            Assert.AreEqual(null, sourceNode1.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "topic" }, sourceNode1.Topics);

            Assert.IsTrue(sourceNode2.Name.StartsWith(KStream.SOURCE_NAME));
            Assert.AreEqual(typeof(FailOnInvalidTimestamp), sourceNode2.TimestampExtractorType);
            Assert.AreEqual(new List<string> { "KTABLE-AGGREGATE-STATE-STORE-0000000005-repartition" }, sourceNode2.Topics);
        }

    }
}
