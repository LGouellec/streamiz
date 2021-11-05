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

        void Commit();

        void Resume();

        void Suspend();

        void Close();

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

        bool HasStateStores { get; }

        PartitionGrouper Grouper { get; }
    }
}
