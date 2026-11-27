using Confluent.Kafka;
using System;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock.Sync;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal class SyncPipeInput : IPipeInput
    {
        private readonly ISyncPublisher publisher;
        private readonly string topic;
        private readonly SyncProducer partitionsSource;
        private readonly Partitioner? partitioner;
        private static readonly object _lock = new();

        public SyncPipeInput(ISyncPublisher publisher, string topic, SyncProducer partitionsSource = null, Partitioner? partitioner = null)
        {
            this.publisher = publisher;
            this.topic = topic;
            this.partitionsSource = partitionsSource;
            this.partitioner = partitioner;
        }

        public string TopicName => topic;
        public event PipeFlushed Flushed;

        public void Dispose()
        {
            Flush();
            publisher.Close();
        }

        public void Flush()
        {
            lock (_lock)
            {
                publisher.Flush();
                Flushed?.Invoke();
            }
        }

        public virtual void Pipe(byte[] key, byte[] value, DateTime timestamp, Headers headers, int partition = -1)
        {
            lock (_lock)
            {
                publisher.PublishRecord(topic, ResolvePartition(key, partition), key, value, timestamp, headers);
            }
        }

        // partition of a piped record: the requested one, or the one chosen by the producer partitioner
        private int ResolvePartition(byte[] key, int partition)
        {
            if (partitionsSource == null)
                return 0;

            int count = partitionsSource.GetPartitionCount(topic);
            if (partition < 0)
                return KafkaPartitioner.ChoosePartition(key, count, partitioner);
            if (partition >= count)
                throw new ArgumentOutOfRangeException(nameof(partition), partition,
                    $"Topic {topic} has {count} partition(s), partition {partition} does not exist");
            return partition;
        }
    }
}
