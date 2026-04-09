using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// Represents a unit of work for processing a single record in parallel.
    /// Wraps a Kafka record with metadata for tracking its processing state.
    /// </summary>
    internal class RecordWorkItem
    {
        /// <summary>
        /// The Kafka record to process.
        /// </summary>
        public ConsumeResult<byte[], byte[]> Record { get; }

        /// <summary>
        /// Task completion source to signal when processing completes.
        /// </summary>
        public TaskCompletionSource<ProcessingResult> CompletionSource { get; }

        /// <summary>
        /// Timestamp when this work item was dispatched to a worker.
        /// </summary>
        public DateTime DispatchedAt { get; }

        /// <summary>
        /// Number of times this record has been retried.
        /// </summary>
        public int RetryCount { get; set; }

        /// <summary>
        /// Creates a new work item for the given record.
        /// </summary>
        /// <param name="record">The Kafka record to process</param>
        public RecordWorkItem(ConsumeResult<byte[], byte[]> record)
        {
            Record = record ?? throw new ArgumentNullException(nameof(record));
            CompletionSource = new TaskCompletionSource<ProcessingResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            DispatchedAt = DateTime.UtcNow;
            RetryCount = 0;
        }

        /// <summary>
        /// Marks this work item as successfully completed.
        /// </summary>
        public void Complete()
        {
            CompletionSource.TrySetResult(ProcessingResult.Success);
        }

        /// <summary>
        /// Marks this work item as failed with the given exception.
        /// </summary>
        /// <param name="exception">The exception that caused the failure</param>
        public void Fail(Exception exception)
        {
            CompletionSource.TrySetException(exception);
        }

        /// <summary>
        /// Marks this work item as cancelled.
        /// </summary>
        public void Cancel()
        {
            CompletionSource.TrySetCanceled();
        }

        /// <summary>
        /// Gets the elapsed time since this work item was dispatched.
        /// </summary>
        public TimeSpan ElapsedTime => DateTime.UtcNow - DispatchedAt;
    }

    /// <summary>
    /// Result of processing a record.
    /// </summary>
    internal enum ProcessingResult
    {
        /// <summary>
        /// Record was processed successfully.
        /// </summary>
        Success,

        /// <summary>
        /// Record processing failed.
        /// </summary>
        Failed,

        /// <summary>
        /// Record processing was skipped (e.g., due to retry policy).
        /// </summary>
        Skipped
    }
}
