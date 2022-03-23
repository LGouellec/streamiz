using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Tests.Metrics.Stats
{
    public class ValueTests
    {
        private Value value;
        private readonly MetricConfig config = new MetricConfig();

        [SetUp]
        public void Init()
        {
            value = new Value();
        }
        
        [Test]
        public void ValueOneRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            this.value.Record(config, 12, now);
            double value = this.value.Measure(config, now);
            Assert.AreEqual(12d, value);
        }
        
        [Test]
        public void ValueNoRecord()
        {
            long now = DateTime.Now.GetMilliseconds();
            double value = this.value.Measure(config, now);
            Assert.AreEqual(0d, value);
        }
        
        [Test]
        public void ValueMultipleRecord()
        {
            double v1 = 12, v2 = 10, v3 = 4;
            long now = DateTime.Now.GetMilliseconds();
            this.value.Record(config, v1, now);
            this.value.Record(config, v2, now);
            this.value.Record(config, v3, now);
            double value = this.value.Measure(config, now);
            Assert.AreEqual(v3, value);
        }
    }
}