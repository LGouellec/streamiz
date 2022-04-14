using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Tests.Metrics.Stats
{
    public class MinTests
    {
        private Min min;
        private readonly MetricConfig config = new MetricConfig();

        [SetUp]
        public void Init()
        {
            min = new Min();
        }
        
        [Test]
        public void MinOneRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            min.Record(config, 12, now);
            double value = min.Measure(config, now);
            Assert.AreEqual(12, value);
        }
        
        [Test]
        public void MinNoRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            double value = min.Measure(config, now);
            Assert.AreEqual(Double.NaN, value);
        }
        
        [Test]
        public void MinMultipleRecord()
        {
            double v1 = 12, v2 = 4, v3 = 10;
            long now = DateTime.Now.GetMilliseconds();
            min.Record(config, v1, now);
            min.Record(config, v2, now);
            min.Record(config, v3, now);
            double value = min.Measure(config, now);
            Assert.AreEqual(v2, value);
        }
        
        [Test]
        public void MinMultipleWithExpiredRecord()
        {
            double v1 = 1, v2 = 10, v3 = 4;
            long now = DateTime.Now.GetMilliseconds();
            min.Record(config, v1, now - (config.Samples * config.TimeWindowMs) - 1000); // expired record
            min.Record(config, v2, now);
            min.Record(config, v3, now);
            double value = min.Measure(config, now);
            Assert.AreEqual(v3, value);
        }
    }
}