using NUnit.Framework;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class SensorTests
    {
        [Test]
        public void AddMetricsOk()
        {
            Sensor sensor = new Sensor("sensor", "description", MetricsRecordingLevel.INFO);
            var metricStatName = MetricName.NameAndGroup("metric-stat-1", "METRIC-GROUP");
            var metricImmutName = MetricName.NameAndGroup("metric-immu-1", "METRIC-GROUP");
            var metricProName = MetricName.NameAndGroup("metric-pro-1", "METRIC-GROUP");
            
            Assert.IsTrue(sensor.AddStatMetric(metricStatName, new Avg()));
            Assert.IsTrue(sensor.AddImmutableMetric(metricImmutName, 1));
            Assert.IsTrue(sensor.AddProviderMetric(metricProName, () => "coucou"));
        }
        
        [Test]
        public void MetricsAlreadyAdded()
        {
            Sensor sensor = new Sensor("sensor", "description", MetricsRecordingLevel.INFO);
            var metricStatName = MetricName.NameAndGroup("metric-stat-1", "METRIC-GROUP");
            var metricImmutName = MetricName.NameAndGroup("metric-immu-1", "METRIC-GROUP");
            var metricProName = MetricName.NameAndGroup("metric-pro-1", "METRIC-GROUP");

            sensor.AddStatMetric(metricStatName, new Avg());
            sensor.AddImmutableMetric(metricImmutName, 1);
            sensor.AddProviderMetric(metricProName, () => "coucou");
            
            Assert.IsFalse(sensor.AddStatMetric(metricStatName, new Avg()));
            Assert.IsFalse(sensor.AddImmutableMetric(metricImmutName, 1));
            Assert.IsFalse(sensor.AddProviderMetric(metricProName, () => "coucou"));
        }
        
        [Test]
        public void MetricsNoRunnable()
        {
            Sensor sensor = new Sensor("sensor", "description", MetricsRecordingLevel.DEBUG);
            sensor.NoRunnable = true;
            var metricStatName = MetricName.NameAndGroup("metric-stat-1", "METRIC-GROUP");
            var metricImmutName = MetricName.NameAndGroup("metric-immu-1", "METRIC-GROUP");
            var metricProName = MetricName.NameAndGroup("metric-pro-1", "METRIC-GROUP");
            
            Assert.IsFalse(sensor.AddStatMetric(metricStatName, new Avg()));
            Assert.IsFalse(sensor.AddImmutableMetric(metricImmutName, 1));
            Assert.IsFalse(sensor.AddProviderMetric(metricProName, () => "coucou"));
        }
    }
}