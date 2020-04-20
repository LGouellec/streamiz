using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class StreamsRebalanceListener : IConsumerRebalanceListener
    {
        private TaskManager manager;
        internal StreamThread Thread { get; set; }

        public StreamsRebalanceListener(TaskManager manager)
        {
            this.manager = manager;
        }

        public void PartitionsAssigned(IConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions)
        {
            manager.CreateTasks(partitions);
            Thread.SetState(ThreadState.PARTITIONS_ASSIGNED);
        }

        public void PartitionsRevoked(IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> partitions)
        {
            manager.RevokeTasks(new List<TopicPartition>(partitions.Select(p => p.TopicPartition)));
            Thread.SetState(ThreadState.PARTITIONS_REVOKED);
        }
    }
}
