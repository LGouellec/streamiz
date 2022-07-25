using System;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    /// <summary>
    /// No runnable sensor for test
    /// </summary>
    internal class NoRunnableSensor : Sensor
    {
        internal NoRunnableSensor(string name, string description, MetricsRecordingLevel metricsRecording) 
            : base(name, description, metricsRecording)
        {
            NoRunnable = true;
        }

        internal static NoRunnableSensor Empty =>
            new NoRunnableSensor("unknown", "unknown", MetricsRecordingLevel.INFO);
    }
}