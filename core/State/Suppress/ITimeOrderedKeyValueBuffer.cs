using System;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.State.Suppress
{
    internal interface ITimeOrderedKeyValueBuffer<K, V, T> : IStateStore
    {
        long NumRecords { get; }
        long BufferSize { get; }
        long MinTimestamp { get; }
        
        bool Put(long timestamp, K key, T value, IRecordContext recordContext);
        ValueAndTimestamp<V> PriorValueForBuffered(K key);
        void EvictWhile(Func<bool> predicate, Action<K, T, IRecordContext> evictHandler);
        void SetSerdesIfNull(ISerDes<K> contextKeySerdes, ISerDes<V> contextValueSerdes);
    }
}