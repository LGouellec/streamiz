using System;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Warning : Will change the name of this class on next release.
    /// <para>
    /// This helper class permit to follow metadata during record processing.
    /// Sometimes, project need to known current timestamp, headers, offset, partition and topic about records. 
    /// If you use DSL APi, you can use this helper class to get current metadata. If none record is processing, methods return null.
    /// </para>
    /// <para>
    /// You can use a use case bellow :
    /// </para>
    /// <para>
    /// <code>
    /// var config = new StreamConfig();
    /// /* ... */
    /// config.FollowMetadata = true;
    /// 
    /// var builder = new StreamBuilder();
    /// builder
    ///         .Stream&lt;string, string&gt;("topic")
    ///         .MapValues((v) => {
    ///             h = StreamizMetadata.GetCurrentPartitionMetadata();
    ///             /* DO SOMETHING WITH METADATA */
    ///             return v;
    ///         })
    ///         .To("output");
    /// </code>
    /// </para>
    /// </summary>
    [Obsolete("Plan to remove in 1.8.0")]
    public static class StreamizMetadata
    {
        /// <summary>
        /// Get current headers metadata about record processing
        /// </summary>
        /// <returns>Return current headers or null if none record is processing.</returns>
        public static Headers GetCurrentHeadersMetadata()
        {
            var task = TaskManager.CurrentTask;
            if (task != null)
            {
                if (task.Context.FollowMetadata)
                    return task.Context.RecordContext.Headers;
                else
                    return null;
            }
            else
                return null;
        }

        /// <summary>
        /// Get current timestamp metadata about record processing
        /// </summary>
        /// <returns>Return current timestamp or null if none record is processing.</returns>
        public static long? GetCurrentTimestampMetadata()
        {
            var task = TaskManager.CurrentTask;
            if (task != null)
            {
                if (task.Context.FollowMetadata)
                    return task.Context.RecordContext.Timestamp;
                else
                    return null;
            }
            else
                return null;
        }

        /// <summary>
        /// Get current offset metadata about record processing
        /// </summary>
        /// <returns>Return current offset or null if none record is processing.</returns>
        public static long? GetCurrentOffsetMetadata()
        {
            var task = TaskManager.CurrentTask;
            if (task != null)
            {
                if (task.Context.FollowMetadata)
                    return task.Context.RecordContext.Offset;
                else
                    return null;
            }
            else
                return null;
        }

        /// <summary>
        /// Get current partition metadata about record processing
        /// </summary>
        /// <returns>Return current partition or null if none record is processing.</returns>
        public static int? GetCurrentPartitionMetadata()
        {
            var task = TaskManager.CurrentTask;
            if (task != null)
            {
                if (task.Context.FollowMetadata)
                    return task.Context.RecordContext.Partition;
                else
                    return null;
            }
            else
                return null;
        }

        /// <summary>
        /// Get current topic metadata about record processing
        /// </summary>
        /// <returns>Return current topic or null if none record is processing.</returns>
        public static string GetCurrentTopicMetadata()
        {
            var task = TaskManager.CurrentTask;
            if (task != null)
            {
                if (task.Context.FollowMetadata)
                    return task.Context.RecordContext.Topic;
                else
                    return null;
            }
            else
                return null;
        }
    }
}