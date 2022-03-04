using System;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class NoRunnableSensor : Sensor
    {
        internal NoRunnableSensor(string name, string description, MetricsRecordingLevel metricsRecording) 
            : base(name, description, metricsRecording)
        {
        }

        protected override void RecordInternal(double value, long timeMs)
        {
            // nothing, this sensor is not runnable because the metris recording level is not compatible with this one passed on configuration
        }

        internal override bool AddStatMetric(MetricName name, IMeasurableStat stat, MetricConfig config = null)
            => false;

        internal override bool AddImmutableMetric<T>(MetricName name, T value, MetricConfig config = null)
            => false;

        internal override bool AddProviderMetric<T>(MetricName name, Func<T> provider, MetricConfig config = null)
            => false;
    }
}