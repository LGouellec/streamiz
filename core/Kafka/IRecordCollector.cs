using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Kafka
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
