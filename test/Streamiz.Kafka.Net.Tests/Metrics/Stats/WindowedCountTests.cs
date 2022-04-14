using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Tests.Metrics.Stats
{
    public class WindowedCountTests
    {
        private WindowedCount windowedCount;
        private readonly MetricConfig config = new MetricConfig();

        [SetUp]
        public void Init()
        {
            windowedCount = new WindowedCount();
        }
        
        [Test]
        public void WindowedSumOneRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            windowedCount.Record(config, 12, now);
            double value = windowedCount.Measure(config, now);
            Assert.AreEqual(1d, value);
        }
        
        [Test]
        public void WindowedSumNoRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            double value = windowedCount.Measure(config, now);
            Assert.AreEqual(0d, value);
        }
        
        [Test]
        public void WindowedSumMultipleRecord()
        {
            double v1 = 12, v2 = 10, v3 = 4;
            long now = DateTime.Now.GetMilliseconds();
            windowedCount.Record(config, v1, now);
            windowedCount.Record(config, v2, now);
            windowedCount.Record(config, v3, now);
            double value = windowedCount.Measure(config, now);
            Assert.AreEqual(3d, value);
        }
        
        [Test]
        public void WindowedSumMultipleWithExpiredRecord()
        {
            double v1 = 12, v2 = 10, v3 = 4;
            long now = DateTime.Now.GetMilliseconds();
            windowedCount.Record(config, v1, now - (config.Samples * config.TimeWindowMs) - 1000); // expired record
            windowedCount.Record(config, v2, now);
            windowedCount.Record(config, v3, now);
            double value = windowedCount.Measure(config, now);
            Assert.AreEqual(2d, value);
        }

    }
}