using Confluent.Kafka;
using Kafka.Streams.Net.Processors.Internal;
using Kafka.Streams.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Processors
{
    internal interface ITask
    {
        bool CanProcess { get; }

        bool CommitNeeded { get; }

        void InitializeTopology();

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

        TopicPartition Partition { get; }

        /**
         * @return any changelog partitions associated with this task
         */
        ICollection<TopicPartition> ChangelogPartitions { get; }

        bool HasStateStores { get; }
    }
}
