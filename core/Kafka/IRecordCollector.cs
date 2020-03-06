using Confluent.Kafka;
using kafka_stream_core.Processors;
using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Kafka
{
    internal interface IRecordCollector
    {
        void Init(IProducer<byte[], byte[]> producer);
        void Flush();
        void Close();
        void Offsets();
        void Send<K, V>(String topic, K key, V value, Headers headers, int partition, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer);
        void Send<K, V>(String topic, K key, V value, Headers headers, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer, StreamPartitioner<K, V> partitioner);
    }
}
