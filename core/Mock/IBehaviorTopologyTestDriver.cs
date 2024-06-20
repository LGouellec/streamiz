using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using System;

namespace Streamiz.Kafka.Net.Mock
{
    internal interface IBehaviorTopologyTestDriver : IDisposable
    {
        bool IsRunning { get;  }
        bool IsStopped { get; }
        bool IsError { get; }
        void StartDriver();
        TestInputTopic<K, V> CreateInputTopic<K, V>(string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes);
        TestOutputTopic<K, V> CreateOutputTopic<K, V>(string topicName, TimeSpan consumeTimeout, ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null);
        TestMultiInputTopic<K, V> CreateMultiInputTopic<K, V>(string[] topics, ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null);
        IStateStore GetStateStore<K, V>(string name);
        void TriggerCommit();
    }
}
