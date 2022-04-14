using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Tests.Metrics.Stats
{
    public class MaxTests
    {
        private Max max;
        private readonly MetricConfig config = new MetricConfig();

        [SetUp]
        public void Init()
        {
            max = new Max();
        }
        
        [Test]
        public void MaxOneRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            max.Record(config, 12, now);
            double value = max.Measure(config, now);
            Assert.AreEqual(12, value);
        }
        
        [Test]
        public void MaxNoRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            double value = max.Measure(config, now);
            Assert.AreEqual(Double.NaN, value);
        }
        
        [Test]
        public void MaxMultipleRecord()
        {
            double v1 = 12, v2 = 4, v3 = 10;
            long now = DateTime.Now.GetMilliseconds();
            max.Record(config, v1, now);
            max.Record(config, v2, now);
            max.Record(config, v3, now);
            double value = max.Measure(config, now);
            Assert.AreEqual(v1, value);
        }
        
        [Test]
        public void MaxMultipleWithExpiredRecord()
        {
            double v1 = 12, v2 = 10, v3 = 4;
            long now = DateTime.Now.GetMilliseconds();
            max.Record(config, v1, now - (config.Samples * config.TimeWindowMs) - 1000); // expired record
            max.Record(config, v2, now);
            max.Record(config, v3, now);
            double value = max.Measure(config, now);
            Assert.AreEqual(v2, value);
        }
    }
}