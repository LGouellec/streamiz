using System;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// Configuration for parallel processing in external stream processing.
    /// Controls the parallelism strategy, concurrency level, and resource limits.
    /// </summary>
    public class ParallelProcessingConfig
    {
        /// <summary>
        /// Default maximum concurrency based on processor count.
        /// </summary>
        private static readonly int DefaultMaxConcurrency = Environment.ProcessorCount;

        /// <summary>
        /// Default maximum queued records.
        /// </summary>
        private const int DefaultMaxQueuedRecords = 10000;

        /// <summary>
        /// Default maximum wait time for in-flight records to complete during shutdown.
        /// </summary>
        private static readonly TimeSpan DefaultMaxWaitForCompletion = TimeSpan.FromSeconds(30);

        /// <summary>
        /// The parallelism strategy to use.
        /// Default: SEQUENTIAL (for backward compatibility).
        /// </summary>
        public ParallelProcessingMode Mode { get; set; } = ParallelProcessingMode.SEQUENTIAL;

        /// <summary>
        /// Maximum number of concurrent workers processing records.
        /// Default depends on mode:
        /// - SEQUENTIAL: 1
        /// - PER_PARTITION: processor count
        /// - PER_KEY: processor count × 2
        /// - UNORDERED: processor count × 4
        /// </summary>
        public int MaxConcurrency { get; set; } = DefaultMaxConcurrency;

        /// <summary>
        /// Maximum number of records that can be queued for processing.
        /// When this limit is reached, the consumer will pause consuming from Kafka.
        /// Default: 10,000
        /// </summary>
        public int MaxQueuedRecords { get; set; } = DefaultMaxQueuedRecords;

        /// <summary>
        /// Maximum time to wait for in-flight records to complete during shutdown.
        /// After this timeout, remaining in-flight records will be abandoned and may be reprocessed.
        /// Default: 30 seconds
        /// </summary>
        public TimeSpan MaxWaitForCompletion { get; set; } = DefaultMaxWaitForCompletion;

        /// <summary>
        /// Validates the configuration and throws if invalid.
        /// </summary>
        /// <exception cref="ArgumentException">Thrown when configuration is invalid</exception>
        public void Validate()
        {
            if (MaxConcurrency <= 0)
                throw new ArgumentException($"{nameof(MaxConcurrency)} must be greater than 0. Got: {MaxConcurrency}");

            if (MaxQueuedRecords <= 0)
                throw new ArgumentException($"{nameof(MaxQueuedRecords)} must be greater than 0. Got: {MaxQueuedRecords}");

            if (MaxWaitForCompletion <= TimeSpan.Zero)
                throw new ArgumentException($"{nameof(MaxWaitForCompletion)} must be greater than zero. Got: {MaxWaitForCompletion}");

            // Sequential mode should have concurrency = 1 for consistency
            if (Mode == ParallelProcessingMode.SEQUENTIAL && MaxConcurrency > 1)
                throw new ArgumentException($"When {nameof(Mode)} is SEQUENTIAL, {nameof(MaxConcurrency)} must be 1. Got: {MaxConcurrency}");
        }

        /// <summary>
        /// Creates a configuration for sequential processing (current behavior).
        /// </summary>
        public static ParallelProcessingConfig Sequential()
        {
            return new ParallelProcessingConfig
            {
                Mode = ParallelProcessingMode.SEQUENTIAL,
                MaxConcurrency = 1
            };
        }

        /// <summary>
        /// Creates a configuration for per-partition parallel processing.
        /// </summary>
        /// <param name="maxConcurrency">Maximum concurrent workers. Default: processor count</param>
        public static ParallelProcessingConfig PerPartition(int? maxConcurrency = null)
        {
            return new ParallelProcessingConfig
            {
                Mode = ParallelProcessingMode.PER_PARTITION,
                MaxConcurrency = maxConcurrency ?? DefaultMaxConcurrency
            };
        }

        /// <summary>
        /// Creates a configuration for per-key parallel processing.
        /// </summary>
        /// <param name="maxConcurrency">Maximum concurrent workers. Default: processor count × 2</param>
        public static ParallelProcessingConfig PerKey(int? maxConcurrency = null)
        {
            return new ParallelProcessingConfig
            {
                Mode = ParallelProcessingMode.PER_KEY,
                MaxConcurrency = maxConcurrency ?? (DefaultMaxConcurrency * 2)
            };
        }

        /// <summary>
        /// Creates a configuration for unordered parallel processing (maximum throughput).
        /// </summary>
        /// <param name="maxConcurrency">Maximum concurrent workers. Default: processor count × 4</param>
        public static ParallelProcessingConfig Unordered(int? maxConcurrency = null)
        {
            return new ParallelProcessingConfig
            {
                Mode = ParallelProcessingMode.UNORDERED,
                MaxConcurrency = maxConcurrency ?? (DefaultMaxConcurrency * 4)
            };
        }
    }
}
