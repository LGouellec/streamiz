using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Tests.Metrics.Stats
{
    public class WindowedSumTests
    {
        private WindowedSum windowedSum;
        private readonly MetricConfig config = new MetricConfig();

        [SetUp]
        public void Init()
        {
            windowedSum = new WindowedSum();
        }
        
        [Test]
        public void WindowedSumOneRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            windowedSum.Record(config, 12, now);
            double value = windowedSum.Measure(config, now);
            Assert.AreEqual(12, value);
        }
        
        [Test]
        public void WindowedSumNoRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            double value = windowedSum.Measure(config, now);
            Assert.AreEqual(0d, value);
        }
        
        [Test]
        public void WindowedSumMultipleRecord()
        {
            double v1 = 12, v2 = 10, v3 = 4;
            long now = DateTime.Now.GetMilliseconds();
            windowedSum.Record(config, v1, now);
            windowedSum.Record(config, v2, now);
            windowedSum.Record(config, v3, now);
            double value = windowedSum.Measure(config, now);
            Assert.AreEqual((v1 + v2 + v3), value);
        }
        
        [Test]
        public void WindowedSumMultipleWithExpiredRecord()
        {
            double v1 = 12, v2 = 10, v3 = 4;
            long now = DateTime.Now.GetMilliseconds();
            windowedSum.Record(config, v1, now - (config.Samples * config.TimeWindowMs) - 1000); // expired record
            windowedSum.Record(config, v2, now);
            windowedSum.Record(config, v3, now);
            double value = windowedSum.Measure(config, now);
            Assert.AreEqual((v2 + v3), value);
        }

    }
}