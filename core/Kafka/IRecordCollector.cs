using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Kafka
{
    internal interface IRecordCollector
    {
        void Init(ref IProducer<byte[], byte[]> producer);
        void Flush();
        void Close();
        void Send<K, V>(string topic, K key, V value, Headers headers, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer);
    }
}
