using System;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// Defines the parallelism strategy for external stream processing.
    /// Each mode offers different trade-offs between ordering guarantees and throughput.
    /// </summary>
    public enum ParallelProcessingMode
    {
        /// <summary>
        /// Sequential processing (current behavior).
        /// Records are processed one at a time in order.
        /// Guarantees: Full ordering (partition, key, and cross-partition order).
        /// Use case: When strict ordering is required or for backward compatibility.
        /// </summary>
        SEQUENTIAL,

        /// <summary>
        /// Parallel processing by partition.
        /// Different partitions are processed concurrently while maintaining order within each partition.
        /// Guarantees: Partition order and key order maintained.
        /// Use case: When partition-level ordering matters but cross-partition order doesn't.
        /// </summary>
        PER_PARTITION,

        /// <summary>
        /// Parallel processing by key.
        /// Different keys are processed concurrently while maintaining order for each key.
        /// Guarantees: Per-key order maintained.
        /// Use case: When key-level ordering matters (e.g., user sessions, entity updates).
        /// </summary>
        PER_KEY,

        /// <summary>
        /// Unordered parallel processing.
        /// Maximum parallelism with no ordering guarantees.
        /// Guarantees: None - records may be processed out of order.
        /// Use case: When order doesn't matter and maximum throughput is needed.
        /// </summary>
        UNORDERED
    }
}
