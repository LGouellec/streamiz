using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;

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
                if (metadata is SyncConsumer consumer)
                    consumer.Commit(offsets);
            }
        }
        private static readonly object _lock = new();
        private readonly Dictionary<string, List<Message<byte[], byte[]>>> topics = new();
        // number of partitions of each topic
        private readonly Dictionary<string, int> partitionCounts = new();
        // partition and offset (in its partition) of each record, by index in the topic history
        private readonly Dictionary<string, List<(int partition, long offset)>> recordPositions = new();
        // next offset of each partition, by topic
        private readonly Dictionary<string, Dictionary<int, long>> nextOffsets = new();
        private int defaultPartitions = 1;
        private SyncTransaction transaction;
        private ProducerConfig config;
        
        public SyncProducer(){}
        public SyncProducer(ProducerConfig config) { this.config = config; }

        public void UseConfig(ProducerConfig config)
        {
            this.config = config;
        }


        /// <summary>
        /// Number of partitions of the topics created without an explicit number of partitions.
        /// </summary>
        internal int DefaultPartitions
        {
            get => defaultPartitions;
            set
            {
                if (value <= 0)
                    throw new ArgumentOutOfRangeException(nameof(value), value, "The number of partitions must be positive");
                defaultPartitions = value;
            }
        }

        internal void CreateTopic(string topicName) => CreateTopic(topicName, -1);

        /// <summary>
        /// Set the number of partitions of a topic, creating it if needed.
        /// </summary>
        internal void SetPartitionCount(string topicName, int partitions)
        {
            lock (_lock)
            {
                CreateTopic(topicName, partitions);
                partitionCounts[topicName] = partitions;
            }
        }

        /// <summary>
        /// Create a topic if it does not exist.
        /// </summary>
        /// <param name="topicName">topic name</param>
        /// <param name="partitions">number of partitions, <see cref="DefaultPartitions"/> when not positive</param>
        internal void CreateTopic(string topicName, int partitions)
        {
            lock (_lock)
            {
                if (!topics.ContainsKey(topicName))
                {
                    topics.Add(topicName, new List<Message<byte[], byte[]>>());
                    recordPositions.Add(topicName, new List<(int, long)>());
                    nextOffsets.Add(topicName, new Dictionary<int, long>());
                    partitionCounts[topicName] = partitions > 0 ? partitions : defaultPartitions;
                }
            }
        }
        
        internal void DeleteTopic(string topicName)
        {
            lock (_lock)
            {
                topics.Remove(topicName);
                recordPositions.Remove(topicName);
                nextOffsets.Remove(topicName);
                partitionCounts.Remove(topicName);
            }
        }

        /// <summary>
        /// Number of partitions of a topic (<see cref="DefaultPartitions"/> if the topic does not exist yet).
        /// </summary>
        internal int GetPartitionCount(string topicName)
        {
            lock (_lock)
                return partitionCounts.TryGetValue(topicName, out var count) ? count : defaultPartitions;
        }

        /// <summary>
        /// Partition and offset (in its partition) of the record at <paramref name="index"/> in the topic history.
        /// </summary>
        internal (int partition, long offset) GetRecordPosition(string topicName, long index)
        {
            lock (_lock)
                return recordPositions.TryGetValue(topicName, out var positions) && index >= 0 && index < positions.Count
                    ? positions[(int)index]
                    : (0, index);
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
            => Produce(new TopicPartition(topic, Partition.Any), message, deliveryHandler);

        public void Produce(TopicPartition topicPartition, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
        {
            var r = Append(topicPartition, message);
            deliveryHandler?.Invoke(r);
        }

        public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(string topic, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
            => ProduceAsync(new TopicPartition(topic, Partition.Any), message, cancellationToken);

        public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(TopicPartition topicPartition, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
        {
            var r = Append(topicPartition, message);
            return Task.FromResult(new DeliveryResult<byte[], byte[]>
            {
                Message = message,
                Status = PersistenceStatus.Persisted,
                TopicPartitionOffset = r.TopicPartitionOffset
            });
        }

        private DeliveryReport<byte[], byte[]> Append(TopicPartition topicPartition, Message<byte[], byte[]> message)
        {
            var topic = topicPartition.Topic;
            CreateTopic(topic);

            int partition;
            long offset;
            lock (_lock)
            {
                // same partition as the Confluent.Kafka producer when no partition is specified
                partition = topicPartition.Partition == Partition.Any
                    ? KafkaPartitioner.ChoosePartition(message.Key, partitionCounts[topic], config?.Partitioner)
                    : topicPartition.Partition.Value;

                var offsets = nextOffsets[topic];
                offsets.TryGetValue(partition, out offset);
                offsets[partition] = offset + 1;
                topics[topic].Add(message);
                recordPositions[topic].Add((partition, offset));
            }

            return new DeliveryReport<byte[], byte[]>
            {
                Message = message,
                Partition = partition,
                Topic = topic,
                Offset = offset,
                Error = new Error(ErrorCode.NoError),
                Status = PersistenceStatus.Persisted
            };
        }

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        {
            transaction.SetData(offsets, groupMetadata);
        }

        #endregion

        public IEnumerable<Message<byte[], byte[]>> GetHistory(string topicName)
        {
            CreateTopic(topicName);
            lock (_lock)
                return topics[topicName].ToArray();
        }

        public IDictionary<string, int> GetAllTopicCounts()
        {
            lock (_lock)
            {
                var result = new Dictionary<string, int>();
                foreach (var kvp in topics)
                    result[kvp.Key] = kvp.Value.Count;
                return result;
            }
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
