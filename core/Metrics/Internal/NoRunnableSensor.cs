using System.Collections.Generic;

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
    }
}