using NUnit.Framework;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KTableGroupByTests
    {
        [Test]
        public void SouldNotAllowSelectorNull()
        {
            var builder = new StreamBuilder();
            var table = builder.Table<string, string>("topic");
            Func<string, string, KeyValuePair<string,string>> selector1 = null;
            IKeyValueMapper<string, string, KeyValuePair<string, string>> selector2 = null;

            Assert.Throws<ArgumentNullException>(() => table.GroupBy(selector1));
            Assert.Throws<ArgumentNullException>(() => table.GroupBy(selector2));
        }
    }
}
