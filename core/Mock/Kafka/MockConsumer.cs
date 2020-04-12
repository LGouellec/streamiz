using Confluent.Kafka;
using kafka_stream_core.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace kafka_stream_core.Mock.Kafka
{
    internal class MockConsumer : IConsumer<byte[], byte[]>
    {
        private readonly string groupId;
        private readonly string clientId;

        public MockConsumer(string groupId, string clientId)
        {
            this.groupId = groupId;
            this.clientId = clientId;
        }

        #region IConsumer Impl

        public string MemberId => groupId;

        public List<TopicPartition> Assignment { get; private set; } = new List<TopicPartition>();

        public List<string> Subscription { get; private set; } = new List<string>();

        public IConsumerGroupMetadata ConsumerGroupMetadata => null;

        public Handle Handle => null;

        public string Name => clientId;

        public int AddBrokers(string brokers) => 0;

        public void Assign(TopicPartition partition)
        {
            throw new NotImplementedException();
        }

        public void Assign(TopicPartitionOffset partition)
        {
            throw new NotImplementedException();
        }

        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
        {
            throw new NotImplementedException();
        }

        public void Assign(IEnumerable<TopicPartition> partitions)
        {
            throw new NotImplementedException();
        }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public List<TopicPartitionOffset> Commit()
        {
            throw new NotImplementedException();
        }

        public void Commit(IEnumerable<TopicPartitionOffset> offsets)
        {
            throw new NotImplementedException();
        }

        public void Commit(ConsumeResult<byte[], byte[]> result)
        {
            throw new NotImplementedException();
        }

        public List<TopicPartitionOffset> Committed(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            Close();
        }

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
        {
            throw new NotImplementedException();
        }

        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void Pause(IEnumerable<TopicPartition> partitions)
        {
            throw new NotImplementedException();
        }

        public Offset Position(TopicPartition partition)
        {
            throw new NotImplementedException();
        }

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void Resume(IEnumerable<TopicPartition> partitions)
        {
            throw new NotImplementedException();
        }

        public void Seek(TopicPartitionOffset tpo)
        {
            throw new NotImplementedException();
        }

        public void StoreOffset(TopicPartitionOffset offset)
        {
            throw new NotImplementedException();
        }

        public void StoreOffset(ConsumeResult<byte[], byte[]> result)
        {
            throw new NotImplementedException();
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            throw new NotImplementedException();
        }

        public void Subscribe(string topic)
        {
            throw new NotImplementedException();
        }

        public void Unassign()
        {
            throw new NotImplementedException();
        }

        public void Unsubscribe()
        {
            throw new NotImplementedException();
        }

        public ConsumeResult<byte[], byte[]> Consume(int millisecondsTimeout)
        {
            throw new NotImplementedException();
        }

        public ConsumeResult<byte[], byte[]> Consume(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public ConsumeResult<byte[], byte[]> Consume(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        internal void SetRebalanceListener(IConsumerRebalanceListener rebalanceListener)
        {
            
        }
    }
}