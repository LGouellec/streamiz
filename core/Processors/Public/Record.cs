using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Public
{
    public class Record<K, V>
    {
        public TopicPartitionOffset TopicPartitionOffset { get; }
        public Headers Headers { get; }
        public Timestamp Timestamp { get; }
        public K Key { get; }
        public V Value { get; set; }
    }
}