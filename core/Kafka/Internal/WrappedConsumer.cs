using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class WrappedConsumer : IConsumer<byte[], byte[]>
    {
        private readonly IConsumer<byte[], byte[]> internalConsumer;
        private readonly KafkaLoggerAdapter loggerAdapter;

        public WrappedConsumer(
            IConsumer<byte[], byte[]> internalConsumer, 
            KafkaLoggerAdapter loggerAdapter)
        {
            this.internalConsumer = internalConsumer;
            this.loggerAdapter = loggerAdapter;
        }

        private void CheckErrorConnection()
        {
            // todo : 
        }

        public void Dispose()
            => internalConsumer.Dispose();

        public int AddBrokers(string brokers)
        {
            CheckErrorConnection();
            return internalConsumer.AddBrokers(brokers);
        }

        public Handle Handle => internalConsumer.Handle;
        public string Name => internalConsumer.Name;
        
        public ConsumeResult<byte[], byte[]> Consume(int millisecondsTimeout)
        {
            CheckErrorConnection();
            return internalConsumer.Consume(millisecondsTimeout);
        }

        public ConsumeResult<byte[], byte[]> Consume(CancellationToken cancellationToken = new CancellationToken())
        {
            CheckErrorConnection();
            return internalConsumer.Consume(cancellationToken);
        }

        public ConsumeResult<byte[], byte[]> Consume(TimeSpan timeout)
        {
            CheckErrorConnection();
            return internalConsumer.Consume(timeout);
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            CheckErrorConnection();
            internalConsumer.Subscribe(topics);
        }

        public void Subscribe(string topic)
        {
            CheckErrorConnection();
            internalConsumer.Subscribe(topic);
        }

        public void Unsubscribe()
        {
            CheckErrorConnection();
            internalConsumer.Unsubscribe();
        }

        public void Assign(TopicPartition partition)
        {
            CheckErrorConnection();
            internalConsumer.Assign(partition);
        }

        public void Assign(TopicPartitionOffset partition)
        {
            CheckErrorConnection();
            internalConsumer.Assign(partition);
        }

        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
        {
            CheckErrorConnection();
            internalConsumer.Assign(partitions);
        }

        public void Assign(IEnumerable<TopicPartition> partitions)
        {
            CheckErrorConnection();
            internalConsumer.Assign(partitions);
        }

        public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions)
        {
            CheckErrorConnection();
            internalConsumer.IncrementalAssign(partitions);
        }

        public void IncrementalAssign(IEnumerable<TopicPartition> partitions)
        {
            CheckErrorConnection();
            internalConsumer.IncrementalAssign(partitions);
        }

        public void IncrementalUnassign(IEnumerable<TopicPartition> partitions)
        {
            CheckErrorConnection();
            internalConsumer.IncrementalUnassign(partitions);
        }

        public void Unassign()
        {
            CheckErrorConnection();
            internalConsumer.Unassign();
        }

        public void StoreOffset(ConsumeResult<byte[], byte[]> result)
        {
            CheckErrorConnection();
            internalConsumer.StoreOffset(result);
        }

        public void StoreOffset(TopicPartitionOffset offset)
        {
            CheckErrorConnection();
            internalConsumer.StoreOffset(offset);
        }

        public List<TopicPartitionOffset> Commit()
        {
            CheckErrorConnection();
            return internalConsumer.Commit();
        }

        public void Commit(IEnumerable<TopicPartitionOffset> offsets)
        {
            CheckErrorConnection();
            internalConsumer.Commit(offsets);
        }

        public void Commit(ConsumeResult<byte[], byte[]> result)
        {
            CheckErrorConnection();
            internalConsumer.Commit(result);
        }

        public void Seek(TopicPartitionOffset tpo)
        {
            CheckErrorConnection();
            internalConsumer.Seek(tpo);
        }

        public void Pause(IEnumerable<TopicPartition> partitions)
        {
            CheckErrorConnection();
            internalConsumer.Pause(partitions);
        }

        public void Resume(IEnumerable<TopicPartition> partitions)
        {
            CheckErrorConnection();
            internalConsumer.Resume(partitions);
        }

        public List<TopicPartitionOffset> Committed(TimeSpan timeout)
        {
            CheckErrorConnection();
            return internalConsumer.Committed(timeout);
        }

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
        {
            CheckErrorConnection();
            return internalConsumer.Committed(partitions, timeout);
        }

        public Offset Position(TopicPartition partition)
        {
            CheckErrorConnection();
            return internalConsumer.Position(partition);
        }

        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
        {
            CheckErrorConnection();
            return internalConsumer.OffsetsForTimes(timestampsToSearch, timeout);
        }

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
        {
            CheckErrorConnection();
            return internalConsumer.GetWatermarkOffsets(topicPartition);
        }

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
        {
            CheckErrorConnection();
            return internalConsumer.QueryWatermarkOffsets(topicPartition, timeout);
        }

        public void Close()
        {
            internalConsumer.Close();
            CheckErrorConnection();
        }

        public string MemberId => internalConsumer.MemberId;
        public List<TopicPartition> Assignment => internalConsumer.Assignment;
        public List<string> Subscription => internalConsumer.Subscription;
        public IConsumerGroupMetadata ConsumerGroupMetadata => internalConsumer.ConsumerGroupMetadata;
    }
}