using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net
{
    public static class StreamizMetadata
    {
        public static Headers GetCurrentHeadersMetadata()
        {
            var task = TaskManager.CurrentTask;
            return task != null ? task.Context?.RecordContext?.Headers : null;
        }

        public static long? GetCurrentTimestampMetadata()
        {
            var task = TaskManager.CurrentTask;
            return task != null ? task.Context?.RecordContext?.Timestamp : null;
        }

        public static long? GetCurrentOffsetMetadata()
        {
            var task = TaskManager.CurrentTask;
            return task != null ? task.Context?.RecordContext?.Offset : null;
        }

        public static int? GetCurrentPartitionMetadata()
        {
            var task = TaskManager.CurrentTask;
            return task != null ? task.Context?.RecordContext?.Partition : null;
        }

        public static string GetCurrentTopicMetadata()
        {
            var task = TaskManager.CurrentTask;
            return task != null ? task.Context?.RecordContext?.Topic : null;
        }
    }
}