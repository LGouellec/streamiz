using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class RocksDBMetricsRecordingTrigger
    {
        internal IDictionary<string, RocksDbMetricsRecorder> MetricsRecorders { get; private set; }

        internal void AddMetricsRecorder(RocksDbMetricsRecorder recorder)
        {
            if(!MetricsRecorders.ContainsKey(recorder.Name))
                MetricsRecorders.Add(recorder.Name, recorder);
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