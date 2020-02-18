using Confluent.Kafka;
using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;
using System;

namespace kafka_stream_core.Kafka.Internal
{
    internal class RecordCollectorImpl : IRecordCollector
    {
        public void Close()
        {
            throw new NotImplementedException();
        }

        public void Flush()
        {
            throw new NotImplementedException();
        }

        public void Init(IProducer<byte[], byte[]> producer)
        {
            throw new NotImplementedException();
        }

        public void Offsets()
        {
            throw new NotImplementedException();
        }

        public void Send<K, V>(string topic, K key, V value, Headers headers, int partition, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer)
        {
            throw new NotImplementedException();
        }

        public void Send<K, V>(string topic, K key, V value, Headers headers, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer, StreamPartitioner<K, V> partitioner)
        {
            throw new NotImplementedException();
        }
    }
}
