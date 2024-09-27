using System.Collections.Generic;
using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Kafka
{
    internal interface IRecordCollector
    {
        IDictionary<TopicPartition, long> CollectorOffsets { get; }
        void Initialize();
        void Flush();
        void Close(bool dirty);
        void Send<K, V>(string topic, K key, V value, Headers headers, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer);
        void Send<K, V>(string topic, K key, V value, Headers headers, int partition, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer);
        int PartitionsFor(string topic);
    }
}
