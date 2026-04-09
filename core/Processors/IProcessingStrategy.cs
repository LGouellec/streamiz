using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// Strategy interface for processing records with different parallelism guarantees.
    /// Implementations provide different trade-offs between ordering and throughput.
    /// </summary>
    internal interface IProcessingStrategy : IDisposable
    {
        /// <summary>
        /// Submits a record for asynchronous processing.
        /// The record will be queued and processed according to the strategy's ordering guarantees.
        /// </summary>
        /// <param name="record">The Kafka record to process</param>
        /// <returns>A task that completes when the record has been processed</returns>
        /// <exception cref="InvalidOperationException">Thrown if the strategy has been closed</exception>
        Task SubmitAsync(ConsumeResult<byte[], byte[]> record);

        /// <summary>
        /// Gets the offsets that can be safely committed to Kafka.
        /// Only returns offsets for records that have completed processing,
        /// respecting the strategy's ordering guarantees to prevent data loss.
        /// </summary>
        /// <returns>Collection of committable topic-partition-offsets</returns>
        IEnumerable<TopicPartitionOffset> GetCommittableOffsets();

        /// <summary>
        /// Flushes any pending producer operations.
        /// Ensures all produced records are sent to Kafka brokers.
        /// </summary>
        void Flush();

        /// <summary>
        /// Closes the strategy and releases resources.
        /// Waits for in-flight records to complete (up to configured timeout).
        /// </summary>
        void Close();

        /// <summary>
        /// Gets the number of records currently being processed or queued.
        /// Used for backpressure and monitoring.
        /// </summary>
        int InFlightCount { get; }

        /// <summary>
        /// Gets the current state of the processing strategy.
        /// </summary>
        ProcessingStrategyState State { get; }
    }

    /// <summary>
    /// Represents the operational state of a processing strategy.
    /// </summary>
    internal enum ProcessingStrategyState
    {
        /// <summary>
        /// Strategy has been created but not yet started.
        /// </summary>
        Created,

        /// <summary>
        /// Strategy is actively processing records.
        /// </summary>
        Running,

        /// <summary>
        /// Strategy is closing and waiting for in-flight records to complete.
        /// </summary>
        Closing,

        /// <summary>
        /// Strategy is closed and no longer accepting records.
        /// </summary>
        Closed
    }
}
