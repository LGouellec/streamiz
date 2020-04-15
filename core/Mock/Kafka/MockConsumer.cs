using Confluent.Kafka;
using Kafka.Streams.Net.Errors;
using Kafka.Streams.Net.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Kafka.Streams.Net.Mock.Kafka
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

        public IConsumerRebalanceListener Listener { get; private set; }

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
            MockCluster.Instance.Assign(this, new List<TopicPartition> { partition });
        }

        public void Assign(TopicPartitionOffset partition)
        {
            // TODO
        }

        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
        {
            // TODO
        }

        public void Assign(IEnumerable<TopicPartition> partitions)
        {
            MockCluster.Instance.Assign(this, partitions);
        }

        public void Close()
        {
            MockCluster.Instance.CloseConsumer(this.Name);
            Assignment.Clear();
            Subscription.Clear();
        }

        public List<TopicPartitionOffset> Commit()
        {
            return MockCluster.Instance.Commit(this);
        }

        public void Commit(IEnumerable<TopicPartitionOffset> offsets)
        {
            MockCluster.Instance.Commit(this, offsets);
        }

        public void Commit(ConsumeResult<byte[], byte[]> result)
            => this.Commit(new List<TopicPartitionOffset> { new TopicPartitionOffset(result.TopicPartition, result.Offset) });

        public List<TopicPartitionOffset> Committed(TimeSpan timeout)
        {
            return MockCluster.Instance.Comitted(this);        
        }

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
        {
            // TODO : 
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            Close();
        }

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
        {
            return MockCluster.Instance.GetWatermarkOffsets(topicPartition);
        }

        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
        {
            // TODO : 
            throw new NotImplementedException();
        }

        public void Pause(IEnumerable<TopicPartition> partitions)
        {
            // TODO : 
        }

        public Offset Position(TopicPartition partition)
        {
            // TODO
            throw new NotImplementedException();
        }

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void Resume(IEnumerable<TopicPartition> partitions)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void Seek(TopicPartitionOffset tpo)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void StoreOffset(TopicPartitionOffset offset)
        { 
            // TODO
            throw new NotImplementedException();
        }

        public void StoreOffset(ConsumeResult<byte[], byte[]> result)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            MockCluster.Instance.SubscribeTopic(this, topics);
            Subscription.AddRange(topics);
        }

        public void Subscribe(string topic)
        {
            MockCluster.Instance.SubscribeTopic(this, new List<string> { topic });
            Subscription.Add(topic);
        }

        public void Unassign()
        {
            MockCluster.Instance.Unassign(this);
            Assignment.Clear();
        }

        public void Unsubscribe()
        {
            MockCluster.Instance.Unsubscribe(this);
            Subscription.Clear();
        }

        public ConsumeResult<byte[], byte[]> Consume(int millisecondsTimeout)
            => Consume(TimeSpan.FromMilliseconds(millisecondsTimeout));

        public ConsumeResult<byte[], byte[]> Consume(CancellationToken cancellationToken)
        {
            if (this.Subscription.Count == 0)
                throw new StreamsException("No subscription have been done !");

            return MockCluster.Instance.Consume(this, cancellationToken);
        }

        public ConsumeResult<byte[], byte[]> Consume(TimeSpan timeout)
        {
            if (this.Subscription.Count == 0)
                throw new StreamsException("No subscription have been done !");
            
            return MockCluster.Instance.Consume(this, timeout);
        }

        #endregion

        internal void SetRebalanceListener(IConsumerRebalanceListener rebalanceListener)
        {
            Listener = new MockWrappedConsumerRebalanceListener(rebalanceListener, this);   
        }

        public void PartitionsAssigned(List<TopicPartition> partitions)
        {
            Assignment.Clear();
            Assignment.AddRange(partitions);
        }

        public void PartitionsRevoked(List<TopicPartitionOffset> partitions)
        {
            foreach (var p in partitions)
                if (Assignment.Contains(p.TopicPartition))
                    Assignment.Remove(p.TopicPartition);
        }
    }
}