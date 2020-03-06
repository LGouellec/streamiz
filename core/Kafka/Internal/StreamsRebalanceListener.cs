using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using kafka_stream_core.Processors;
using kafka_stream_core.Processors.Internal;

namespace kafka_stream_core.Kafka.Internal
{
    internal class StreamsRebalanceListener : IConsumerRebalanceListener
    {
        private TaskManager manager;
        private StreamThread thread;

        public StreamsRebalanceListener(TaskManager manager)
        {
            this.manager = manager;
        }

        public StreamsRebalanceListener UseStreamThread(StreamThread thread)
        {
            this.thread = thread;
            return this;
        }

        public void PartitionsAssigned(IConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions)
        {
            manager.CreateTasks(partitions);
            thread.SetState(StreamThread.State.PARTITIONS_ASSIGNED);
        }

        public void PartitionsRevoked(IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> partitions)
        {
            manager.RevokeTasks(new List<TopicPartition>(partitions.Select(p => p.TopicPartition)));
            thread.SetState(StreamThread.State.PARTITIONS_REVOKED);
        }
    }
}
