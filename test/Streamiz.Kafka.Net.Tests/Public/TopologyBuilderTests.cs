using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class TopologyBuilderTests
    {
        [Test]
        public void SourceTopicAlreadyAdded()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("table");
            builder.Stream<string, string>("table");
            Assert.Throws<TopologyException>(() => builder.Build());
        }
    }
}
