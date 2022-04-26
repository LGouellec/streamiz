/*using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class WrappedProducer : IProducer<byte[], byte[]>
    {
        private readonly IProducer<byte[], byte[]> internalProducer;
        private readonly KafkaLoggerAdapter loggerAdapter;

        public WrappedProducer(
            IProducer<byte[], byte[]> internalProducer, 
            KafkaLoggerAdapter loggerAdapter)
        {
            this.internalProducer = internalProducer;
            this.loggerAdapter = loggerAdapter;
        }

        private void CheckErrorConnection()
        {
            // todo : 
        }
        
        public void Dispose()
            => internalProducer.Dispose();

        public int AddBrokers(string brokers)
        {
            CheckErrorConnection();
            return internalProducer.AddBrokers(brokers);
        }

        public Handle Handle => internalProducer.Handle;
        public string Name => internalProducer.Name;
        
        public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(string topic, Message<byte[], byte[]> message, CancellationToken cancellationToken = new CancellationToken())
        {
            CheckErrorConnection();
            return internalProducer.ProduceAsync(topic, message, cancellationToken);
        }

        public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(TopicPartition topicPartition, Message<byte[], byte[]> message,
            CancellationToken cancellationToken = new CancellationToken())
        {
            CheckErrorConnection();
            return internalProducer.ProduceAsync(topicPartition, message, cancellationToken);
        }

        public void Produce(string topic, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
        {
            CheckErrorConnection();
            internalProducer.Produce(topic, message, deliveryHandler);
        }

        public void Produce(TopicPartition topicPartition, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
        {
            CheckErrorConnection();
            internalProducer.Produce(topicPartition, message, deliveryHandler);
        }

        public int Poll(TimeSpan timeout)
        {
            CheckErrorConnection();
            return internalProducer.Poll(timeout);
        }

        public int Flush(TimeSpan timeout)
        {
            CheckErrorConnection();
            return internalProducer.Flush(timeout);
        }

        public void Flush(CancellationToken cancellationToken = new CancellationToken())
        {
            CheckErrorConnection();
            internalProducer.Flush(cancellationToken);
        }

        public void InitTransactions(TimeSpan timeout)
        {
            CheckErrorConnection();
            internalProducer.InitTransactions(timeout);
        }

        public void BeginTransaction()
        {
            CheckErrorConnection();
            internalProducer.BeginTransaction();
        }

        public void CommitTransaction(TimeSpan timeout)
        {
            CheckErrorConnection();
            internalProducer.CommitTransaction(timeout);
        }

        public void CommitTransaction()
        {
            CheckErrorConnection();
            internalProducer.CommitTransaction();
        }

        public void AbortTransaction(TimeSpan timeout)
        {
            CheckErrorConnection();
            internalProducer.AbortTransaction(timeout);
        }

        public void AbortTransaction()
        {
            CheckErrorConnection();
            internalProducer.AbortTransaction();
        }

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        {
            CheckErrorConnection();
            internalProducer.SendOffsetsToTransaction(offsets, groupMetadata, timeout);
        }
    }
}*/