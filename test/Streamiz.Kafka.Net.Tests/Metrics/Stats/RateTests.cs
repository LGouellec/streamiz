using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Tests.Metrics.Stats
{
    public class RateTests
    {
        private Rate rate;
        private readonly MetricConfig config = new MetricConfig();

        [SetUp]
        public void Init()
        {
            rate = new Rate();
        }
        
        [Test]
        public void RateOneRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            rate.Record(config, 12, now);
            double value = rate.Measure(config, now);
            Assert.AreEqual(12d / rate.TimeUnit.Convert(config.TimeWindowMs), value);
        }
        
        [Test]
        public void RateNoRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            double value = rate.Measure(config, now);
            Assert.AreEqual(0d, value);
        }
        
        [Test]
        public void RateMultipleRecord()
        {
            double v1 = 12, v2 = 10, v3 = 4;
            long now = DateTime.Now.GetMilliseconds();
            rate.Record(config, v1, now);
            rate.Record(config, v2, now);
            rate.Record(config, v3, now);
            double value = rate.Measure(config, now);
            Assert.AreEqual((v1 + v2 + v3) / rate.TimeUnit.Convert(config.TimeWindowMs), value);
        }
        
        [Test]
        public void RateMultipleWithExpiredRecord()
        {
            double v1 = 12, v2 = 10, v3 = 4;
            long now = DateTime.Now.GetMilliseconds();
            rate.Record(config, v1, now - (config.Samples * config.TimeWindowMs) - 1000); // expired record
            rate.Record(config, v2, now);
            rate.Record(config, v3, now);
            double value = rate.Measure(config, now);
            Assert.AreEqual((v2 + v3) / rate.TimeUnit.Convert(config.TimeWindowMs), value);
        }

    }
}