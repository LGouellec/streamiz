using System.ComponentModel.DataAnnotations;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Public
{
    public class Record<K, V>
    {
        public TopicPartitionOffset TopicPartitionOffset { get; }
        public Headers Headers { get; }
        public Timestamp Timestamp { get; }
        public K Key { get; }
        public V Value { get; }

        internal Record(TopicPartitionOffset topicPartitionOffset, Headers headers, Timestamp timestamp, K key, V value)
        {
            TopicPartitionOffset = topicPartitionOffset;
            Headers = headers;
            Timestamp = timestamp;
            Key = key;
            Value = value;
        }

        private Record(K key, V value)
        {
            Key = key;
            Value = value;
        }

        private Record(V value)
        {
            Value = value;
        }

        public static Record<K, V> Create(K key, V value)
            => new(key, value);

        public static Record<K, V> Create(V value)
            => new(value);
    }
}