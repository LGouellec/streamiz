using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal class MetricConfig
    {
        public int Samples { get; set; }
        public long EventWindow { get; set; }
        public long TimeWindowMs { get; set; }
        public IDictionary<string, string> Tags { get; set; }
        public MetricsRecordingLevel RecordingLevel { get; set; }

        public MetricConfig()
        {
            Samples = 2;
            EventWindow = long.MaxValue;
            TimeWindowMs = (long)TimeSpan.FromSeconds(30).TotalMilliseconds;
            Tags = new Dictionary<string, string>();
            RecordingLevel = MetricsRecordingLevel.INFO;
        }
        
        #region Builders

        public MetricConfig SetEventWindow(long window)
        {
            EventWindow = window;
            return this;
        }
        
        public MetricConfig SetTimeWindow(TimeSpan window)
        {
            TimeWindowMs = (long)window.TotalMilliseconds;
            return this;
        }
        
        public MetricConfig SetTags(IDictionary<string, string> tags)
        {
            Tags = tags;
            return this;
        }
        
        public MetricConfig SetSamples(int samples)
        {
            Samples = samples;
            return this;
        }
        
        public MetricConfig SetRecordingLevel(MetricsRecordingLevel level)
        {
            RecordingLevel = level;
            return this;
        }
        
        #endregion
    }
}