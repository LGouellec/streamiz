using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Kafka
{
    internal interface IKafkaClient
    {
        void Publish<K, V>(K key, V value, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, string topicName);
        void Subscribe<K, V>(string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, Action<K, V> action);
        void Dispose();
        void Unsubscribe<K, V>(string topicName);
    }
}
