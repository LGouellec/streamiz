using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    // TODO FINISH IMPLEMENTATIONS

    internal class SyncProducer : IProducer<byte[], byte[]>
    {
        private readonly static object _lock = new object();
        private readonly Dictionary<string, List<Message<byte[], byte[]>>> topics = new Dictionary<string, List<Message<byte[], byte[]>>>();

        private ProducerConfig config;

        public SyncProducer(ProducerConfig config)
        {
            this.config = config;
        }

        private void CreateTopic(string topicName)
        {
            lock (_lock)
            {
                if (!topics.ContainsKey(topicName))
                    topics.Add(topicName, new List<Message<byte[], byte[]>>());
            }
        }

        public bool IsMatch(string topicName)
        {
            lock (_lock) { return topics.ContainsKey(topicName); }
        }

        #region IProducer Impl

        public Handle Handle => null;

        public string Name => $"{config.ClientId}#{new Random(DateTime.Now.Millisecond).Next(0, Int16.MaxValue)}";

        public void AbortTransaction(TimeSpan timeout)
        {
            // TODO : 
        }

        public int AddBrokers(string brokers) => 0;

        public void BeginTransaction()
        {
            // TODO
        }

        public void CommitTransaction(TimeSpan timeout)
        {
            // TODO
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
            // TODO : 
        }

        public int Poll(TimeSpan timeout) => 0;


        public void Produce(string topic, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
        {
            CreateTopic(topic);

            this.topics[topic].Add(message);

            DeliveryReport<byte[], byte[]> r = new DeliveryReport<byte[], byte[]>();

            r.Message = message;
            r.Partition = 0;
            r.Topic = topic;
            r.Timestamp = new Timestamp(DateTime.Now);
            r.Error = new Error(ErrorCode.NoError);
            r.Status = PersistenceStatus.Persisted;
            deliveryHandler?.Invoke(r);
        }

        public void Produce(TopicPartition topicPartition, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
            => Produce(topicPartition.Topic, message, deliveryHandler);

        public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(string topic, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
        {
            this.Produce(topic, message);
            return Task.FromResult(new DeliveryResult<byte[], byte[]>() { Message = message, Status = PersistenceStatus.Persisted });
        }

        public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(TopicPartition topicPartition, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
        {
            this.Produce(topicPartition.Topic, message);
            return Task.FromResult(new DeliveryResult<byte[], byte[]>() { Message = message, Status = PersistenceStatus.Persisted });
        }

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        {
            // TODO
        }

        #endregion

        public IEnumerable<Message<byte[], byte[]>> GetHistory(string topicName)
        {
            CreateTopic(topicName);
            return topics[topicName].ToArray();
        }
    }
}
