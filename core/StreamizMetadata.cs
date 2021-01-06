using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net
{
    public static class StreamizMetadata
    {
        public static Headers GetCurrentHeadersMetadata()
        {
            var task = TaskManager.CurrentTask;
            if (task != null)
            {
                if (task.Context.FollowMetadata)
                    return task.Context?.RecordContext?.Headers;
                else
                    return null;
            }
            else
                return null;
        }

        public static long? GetCurrentTimestampMetadata()
        {
            var task = TaskManager.CurrentTask;
            if (task != null)
            {
                if (task.Context.FollowMetadata)
                    return task.Context?.RecordContext?.Timestamp;
                else
                    return null;
            }
            else
                return null;
        }

        public static long? GetCurrentOffsetMetadata()
        {
            var task = TaskManager.CurrentTask;
            if (task != null)
            {
                if (task.Context.FollowMetadata)
                    return task.Context?.RecordContext?.Offset;
                else
                    return null;
            }
            else
                return null;
        }

        public static int? GetCurrentPartitionMetadata()
        {
            var task = TaskManager.CurrentTask;
            if (task != null)
            {
                if (task.Context.FollowMetadata)
                    return task.Context?.RecordContext?.Partition;
                else
                    return null;
            }
            else
                return null;
        }

        public static string GetCurrentTopicMetadata()
        {
            var task = TaskManager.CurrentTask;
            if (task != null)
            {
                if (task.Context.FollowMetadata)
                    return task.Context?.RecordContext?.Topic;
                else
                    return null;
            }
            else
                return null;
        }
    }
}