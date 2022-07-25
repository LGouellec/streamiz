using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Tests.Metrics.Stats
{
    public class CumulativeSumTests
    {
        private CumulativeSum cumSum;
        private readonly MetricConfig config = new MetricConfig();

        [SetUp]
        public void Init()
        {
            cumSum = new CumulativeSum();
        }
        
        [Test]
        public void CumulativeSumOneRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            cumSum.Record(config, 12, now);
            double value = cumSum.Measure(config, now);
            Assert.AreEqual(12, value);
        }
        
        [Test]
        public void CumulativeSumNoRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            double value = cumSum.Measure(config, now);
            Assert.AreEqual(0d, value);
        }
        
        [Test]
        public void CumulativeSumMultipleRecord()
        {
            double v1 = 12, v2 = 4, v3 = 10;
            long now = DateTime.Now.GetMilliseconds();
            cumSum.Record(config, v1, now);
            cumSum.Record(config, v2, now);
            cumSum.Record(config, v3, now);
            double value = cumSum.Measure(config, now);
            Assert.AreEqual(26d, value);
        }
    }
}