using NUnit.Framework;
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
