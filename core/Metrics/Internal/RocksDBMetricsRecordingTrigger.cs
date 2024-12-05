using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class RocksDbMetricsRecordingTrigger
    {
        private ConcurrentDictionary<string, RocksDbMetricsRecorder> MetricsRecorders { get; } = new();

        internal void AddMetricsRecorder(RocksDbMetricsRecorder recorder)
        {
            if (!MetricsRecorders.ContainsKey(recorder.Name))
            {
                MetricsRecorders.TryAdd(recorder.Name, recorder);
                return;
            }

            throw new IllegalStateException(
                $"RocksDB metrics recorder for store {recorder.Name} has already been added.");
        }

        internal void RemoveMetricsRecorder(RocksDbMetricsRecorder recorder)
            => MetricsRecorders.TryRemove(recorder.Name, out RocksDbMetricsRecorder _);

        internal void Run(long now)
        {
            foreach(var recorder in MetricsRecorders.Values)
                recorder.Record(now);
        }
    }
}