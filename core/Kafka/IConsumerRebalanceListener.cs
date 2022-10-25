using Confluent.Kafka;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Kafka
{
    /// <summary>
    /// A callback interface that the user can implement to trigger custom actions when the set of partitions assigned to the consumer changes.
    /// </summary>
    public interface IConsumerRebalanceListener
    {
        /// <summary>
        /// This handler is called when a new consumer group partition assignment has been
        /// received by this consumer. Note: corresponding to every call to this handler
        /// there will be a corresponding call to the partitions revoked handler (if one
        /// has been set using SetPartitionsRevokedHandler"). Consumption will resume from
        /// the last committed offset for each partition, or if there is no committed offset,
        /// in accordance with the `auto.offset.reset` configuration property.
        /// <para>
        /// May execute as a side-effect of the Consumer.Consume call (on the same thread).
        /// Assign/Unassign must not be called in the handler.
        /// </para>
        /// </summary>
        /// <paramref name="consumer">consumer handle</paramref>
        /// <paramref name="partitions">list of partitions assigned</paramref>
        void PartitionsAssigned(IConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions);

        /// <summary>
        /// This handler is called immediately prior to a group partition assignment being
        /// revoked. The second parameter provides the set of partitions the consumer is
        /// currently assigned to, and the current position of the consumer on each of these
        /// partitions. The return value of the handler specifies the partitions/offsets
        /// the consumer should be assigned to following completion of this method (typically
        /// empty).
        /// <para>
        /// May execute as a side-effect of the Consumer.Consume call (on the same thread).
        ///  Assign/Unassign must not be called in the handler
        /// </para>
        /// </summary>
        /// <param name="consumer">consumer handle</param>
        /// <param name="partitions">list of partitions revoked</param>
        void PartitionsRevoked(IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> partitions);
        
        /// <summary>
        /// A callback method you can implement to provide handling of cleaning up resources for partitions that have already
        /// been reassigned to other consumers. This method will not be called during normal execution as the owned partitions would
        /// first be revoked by calling the <see cref="PartitionsRevoked"/>, before being reassigned
        /// to other consumers during a rebalance event. However, during exceptional scenarios when the consumer realized that it
        /// does not own this partition any longer, i.e. not revoked via a normal rebalance event, then this method would be invoked.
        /// </summary>
        /// <param name="consumer">consumer handle</param>
        /// <param name="partitions">The list of partitions that were assigned to the consumer and now have been reassigned to other consumers.</param>
        void PartitionsLost(IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> partitions);
    }
}
