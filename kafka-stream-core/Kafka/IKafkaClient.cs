using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Kafka
{
    internal interface IKafkaClient
    {
        void Publish<K, V>(K key, V value, string topicName);
        void Subscribe<K, V>(string topicName, Action<K, V> action);
        void Dispose();
        void Unsubscribe<K, V>(string topicName);
    }
}
