using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Tests.Metrics.Stats
{
    public class ImmutableMetricValueTests
    {
        private ImmutableMetricValue<string> immutableMetricValue;
        private readonly MetricConfig config = new MetricConfig();
        private static readonly string value = "hello"; 

        [SetUp]
        public void Init()
        {
            immutableMetricValue = new ImmutableMetricValue<string>(value);
        }
        
        [Test]
        public void ImmutableMetricValue()
        {
            long now = DateTime.Now.GetMilliseconds();
            var v = immutableMetricValue.Value(config, now);
            Assert.AreEqual(value, v);
        }
    }
}