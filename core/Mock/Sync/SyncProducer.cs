using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    // TODO FINISH IMPLEMENTATIONS

    internal class SyncProducer : IProducer<byte[], byte[]>
    {
        private class SyncTransaction
        {
            private IEnumerable<TopicPartitionOffset> offsets;
            private IConsumerGroupMetadata metadata;

            public void SetData(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata metadata)
            {
                this.offsets = offsets;
                this.metadata = metadata;
            }

            public void Commit()
            {
                if (metadata is SyncConsumer)
                    (metadata as SyncConsumer).Commit(offsets);
            }
        }

        private readonly static object _lock = new object();
        private readonly Dictionary<string, List<Message<byte[], byte[]>>> topics = new Dictionary<string, List<Message<byte[], byte[]>>>();

        private SyncTransaction transaction = null;

        private ProducerConfig config;

        public SyncProducer(){}
        public SyncProducer(ProducerConfig config) { this.config = config; }

        public void UseConfig(ProducerConfig config)
        {
            this.config = config;
        }


        internal void CreateTopic(string topicName)
        {
            lock (_lock)
            {
                if (!topics.ContainsKey(topicName))
                {
                    topics.Add(topicName, new List<Message<byte[], byte[]>>());
                }
            }
        }

        internal IEnumerable<string> GetAllTopics()
            => topics.Keys;

        public bool IsMatch(string topicName)
        {
            lock (_lock) { return topics.ContainsKey(topicName); }
        }

        #region IProducer Impl
        
        public Handle Handle => null;

        public string Name => $"{config.ClientId}#{RandomGenerator.GetInt32(Int16.MaxValue)}";

        public void AbortTransaction(TimeSpan timeout)
        {
            transaction = null;
        }

        public int AddBrokers(string brokers) => 0;

        public void BeginTransaction()
        {
            transaction = new SyncTransaction();
        }

        public void CommitTransaction(TimeSpan timeout)
        {
            transaction.Commit();
        }

        public void Dispose()
        {
            // TODO
        }

        public int Flush(TimeSpan timeout)
        {
            // TODO
            return 0;
        }

        public void Flush(CancellationToken cancellationToken = default)
        {
            // TODO : 
        }

        public void InitTransactions(TimeSpan timeout)
        {
            transaction = new SyncTransaction();
        }

        public int Poll(TimeSpan timeout) => 0;

        public void Produce(string topic, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
        {
            CreateTopic(topic);

            topics[topic].Add(message);

            DeliveryReport<byte[], byte[]> r = new DeliveryReport<byte[], byte[]>();

            r.Message = message;
            r.Partition = 0;
            r.Topic = topic;
            r.Offset = topics[topic].Count - 1;
            r.Error = new Error(ErrorCode.NoError);
            r.Status = PersistenceStatus.Persisted;
            deliveryHandler?.Invoke(r);
        }

        public void Produce(TopicPartition topicPartition, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
            => Produce(topicPartition.Topic, message, deliveryHandler);

        public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(string topic, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
        {
            Produce(topic, message);
            return Task.FromResult(new DeliveryResult<byte[], byte[]>() { Message = message, Status = PersistenceStatus.Persisted });
        }

        public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(TopicPartition topicPartition, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
        {
            Produce(topicPartition.Topic, message);
            return Task.FromResult(new DeliveryResult<byte[], byte[]>() { Message = message, Status = PersistenceStatus.Persisted });
        }

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        {
            transaction.SetData(offsets, groupMetadata);
        }

        #endregion

        public IEnumerable<Message<byte[], byte[]>> GetHistory(string topicName)
        {
            CreateTopic(topicName);
            return topics[topicName].ToArray();
        }

        public void CommitTransaction()
        {
            throw new NotImplementedException();
        }

        public void AbortTransaction()
        {
            throw new NotImplementedException();
        }
        
        public void SetSaslCredentials(string username, string password)
        {
            
        }
    }
}
