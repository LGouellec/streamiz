using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface ITask
    {
        TaskState State { get; }

        bool IsClosed { get; }

        bool CanProcess(long now);

        bool CommitNeeded { get; }

        void InitializeTopology();

        void RestorationIfNeeded();

        bool InitializeStateStores();

        IEnumerable<TopicPartitionOffset> PrepareCommit();

        void PostCommit(bool enforceCheckpoint);
        
        void Suspend();

        void Close(bool dirty);

        IStateStore GetStore(String name);

        String ApplicationId { get; }

        ProcessorTopology Topology { get; }

        ProcessorContext Context { get; }

        TaskId Id { get; }

        IEnumerable<TopicPartition> Partition { get; }

        /// <summary>
        /// Any changelog partitions associated with this task
        /// </summary>        
        ICollection<TopicPartition> ChangelogPartitions { get; }

        /// <summary>
        /// Any repartitions records to purge after processing and committing
        /// </summary>
        IDictionary<TopicPartition, long> PurgeOffsets { get;  }

        bool HasStateStores { get; }

        PartitionGrouper Grouper { get; }
    }
}
