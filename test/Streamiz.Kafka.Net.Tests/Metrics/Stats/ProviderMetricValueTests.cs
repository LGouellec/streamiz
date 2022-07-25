using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Tests.Metrics.Stats
{
    public class ProviderMetricValueTests
    {
        private ProviderMetricValue<string> providerMetricValue;
        private readonly MetricConfig config = new MetricConfig();
        private static readonly string value = "hello"; 

        [SetUp]
        public void Init()
        {
            providerMetricValue = new ProviderMetricValue<string>(() => value);
        }
        
        [Test]
        public void ProviderMetricValue()
        {
            long now = DateTime.Now.GetMilliseconds();
            var v = providerMetricValue.Value(config, now);
            Assert.AreEqual(value, v);
        }
    }
}