using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Tests.Metrics.Stats
{
    public class CumulativeCountTests
    {
        private CumulativeCount cumCount;
        private readonly MetricConfig config = new MetricConfig();

        [SetUp]
        public void Init()
        {
            cumCount = new CumulativeCount();
        }
        
        [Test]
        public void CumulativeSumOneRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            cumCount.Record(config, 12, now);
            double value = cumCount.Measure(config, now);
            Assert.AreEqual(1d, value);
        }
        
        [Test]
        public void CumulativeSumNoRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            double value = cumCount.Measure(config, now);
            Assert.AreEqual(0d, value);
        }
        
        [Test]
        public void CumulativeSumMultipleRecord()
        {
            double v1 = 12, v2 = 4, v3 = 10;
            long now = DateTime.Now.GetMilliseconds();
            cumCount.Record(config, v1, now);
            cumCount.Record(config, v2, now);
            cumCount.Record(config, v3, now);
            double value = cumCount.Measure(config, now);
            Assert.AreEqual(3d, value);
        }
    }
}