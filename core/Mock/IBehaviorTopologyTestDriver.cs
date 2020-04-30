using Streamiz.Kafka.Net.SerDes;
using System;

namespace Streamiz.Kafka.Net.Mock
{
    internal interface IBehaviorTopologyTestDriver : IDisposable
    {
        void StartDriver();
        TestInputTopic<K, V> CreateInputTopic<K, V>(string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes);
        TestOutputTopic<K, V> CreateOutputTopic<K, V>(string topicName, TimeSpan consumeTimeout, ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null);
    }
}
