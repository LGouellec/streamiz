using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_stream_core.Mock.Kafka
{
    internal class MockProducer : IProducer<byte[], byte[]>
    {
        #region IProducer Impl

        public Handle Handle => throw new NotImplementedException();

        public string Name => throw new NotImplementedException();

        public void AbortTransaction(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public int AddBrokers(string brokers)
        {
            throw new NotImplementedException();
        }

        public void BeginTransaction()
        {
            throw new NotImplementedException();
        }

        public void CommitTransaction(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public int Flush(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void Flush(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public void InitTransactions(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public int Poll(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public void Produce(string topic, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
        {
            throw new NotImplementedException();
        }

        public void Produce(TopicPartition topicPartition, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
        {
            throw new NotImplementedException();
        }

        public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(string topic, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(TopicPartition topicPartition, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
