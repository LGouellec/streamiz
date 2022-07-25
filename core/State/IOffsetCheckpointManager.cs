using System.Collections.Generic;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// This interface save out a map of topic/partition=offsets.
    /// This map track the changelog topic partition for each state store (logging enabled). The default behavior tracks the offsets into a file <see cref="OffsetCheckpointFile"/>.
    /// You can override using <see cref="IStreamConfig.OffsetCheckpointManager"/>.
    /// </summary>
    public interface IOffsetCheckpointManager
    {
        /// <summary>
        /// Read the offsets from source data.
        /// </summary>
        /// <param name="taskId">current task</param>
        /// <returns>topic/partition, offset dictionary</returns>
        IDictionary<TopicPartition, long> Read(TaskId taskId);
        
        /// <summary>
        /// Configure this checkpoint manager.
        /// </summary>
        /// <param name="config">current stream config</param>
        /// <param name="taskId">current task</param>
        void Configure(IStreamConfig config, TaskId taskId);
        
        /// <summary>
        /// Write the given offsets.
        /// </summary>
        /// <param name="taskId">current task</param>
        /// <param name="data">topic/partition, offset dictionary</param>
        void Write(TaskId taskId, IDictionary<TopicPartition, long> data);
        
        /// <summary>
        /// Destroy offsets from current task
        /// </summary>
        /// <param name="taskId">current task</param>
        void Destroy(TaskId taskId);
    }
}