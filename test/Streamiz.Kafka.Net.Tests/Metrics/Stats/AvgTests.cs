using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Tests.Metrics.Stats
{
    public class AvgTests
    {
        private Avg avg;
        private readonly MetricConfig config = new MetricConfig();

        [SetUp]
        public void Init()
        {
            avg = new Avg();
        }
        
        [Test]
        public void AvgOneRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            avg.Record(config, 12, now);
            double value = avg.Measure(config, now);
            Assert.AreEqual(12, value);
        }
        
        [Test]
        public void AvgNoRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            double value = avg.Measure(config, now);
            Assert.AreEqual(Double.NaN, value);
        }
        
        [Test]
        public void AvgMultipleRecord()
        {
            double v1 = 12, v2 = 10, v3 = 4;
            long now = DateTime.Now.GetMilliseconds();
            avg.Record(config, v1, now);
            avg.Record(config, v2, now);
            avg.Record(config, v3, now);
            double value = avg.Measure(config, now);
            Assert.AreEqual((v1 + v2 + v3) / 3, value);
        }
        
        [Test]
        public void AvgMultipleWithExpiredRecord()
        {
            double v1 = 12, v2 = 10, v3 = 4;
            long now = DateTime.Now.GetMilliseconds();
            avg.Record(config, v1, now - (config.Samples * config.TimeWindowMs) - 1000); // expired record
            avg.Record(config, v2, now);
            avg.Record(config, v3, now);
            double value = avg.Measure(config, now);
            Assert.AreEqual((v2 + v3) / 2, value);
        }
    }
}