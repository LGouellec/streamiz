using Confluent.Kafka;
using Kafka.Streams.Net.Processors;
using Kafka.Streams.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Kafka
{
    internal interface IRecordCollector
    {
        void Init(IProducer<byte[], byte[]> producer);
        void Flush();
        void Close();
        void Offsets();
        void Send<K, V>(String topic, K key, V value, Headers headers, int partition, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer);
        void Send<K, V>(String topic, K key, V value, Headers headers, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer, IStreamPartitioner<K, V> partitioner);
    }
}
