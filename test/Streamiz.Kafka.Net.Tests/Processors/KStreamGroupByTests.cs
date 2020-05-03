using NUnit.Framework;
using Streamiz.Kafka.Net.Stream;
using System;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamGroupByTests
    {
        [Test]
        public void SouldNotAllowSelectorNull()
        {
            var builder = new StreamBuilder();
            IKStream<string, string> stream = builder.Stream<string, string>("topic");
            Func<string, string, string> selector1 = null;
            IKeyValueMapper<string, string, string> selector2 = null;

            Assert.Throws<ArgumentNullException>(() => stream.GroupBy(selector1));
            Assert.Throws<ArgumentNullException>(() => stream.GroupBy(selector2));
        }
    }
}
