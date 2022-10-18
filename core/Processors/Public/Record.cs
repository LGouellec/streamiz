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
    }
}