using System.Collections.Generic;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class RocksDbMetricsRecordingTrigger
    {
        private IDictionary<string, RocksDbMetricsRecorder> MetricsRecorders { get; } 
            = new Dictionary<string, RocksDbMetricsRecorder>();

        internal void AddMetricsRecorder(RocksDbMetricsRecorder recorder)
        {
            if (!MetricsRecorders.ContainsKey(recorder.Name))
            {
                MetricsRecorders.Add(recorder.Name, recorder);
                return;
            }

            throw new IllegalStateException(
                $"RocksDB metrics recorder for store {recorder.Name} has already been added.");
        }

        internal void RemoveMetricsRecorder(RocksDbMetricsRecorder recorder)
            => MetricsRecorders.Remove(recorder.Name);

        internal void Run(long now)
        {
            foreach(var recorder in MetricsRecorders.Values)
                recorder.Record(now);
        }
    }
}