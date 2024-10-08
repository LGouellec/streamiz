using System;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State.Suppress
{
    internal interface ITimeOrderedKeyValueBuffer<K, V, T> : IStateStore
    {
        long NumRecords { get; }
        long BufferSize { get; }
        long MinTimestamp { get; }
        
        bool Put(long timestamp, K key, V value, IRecordContext recordContext);
        ValueAndTimestamp<T> PriorValueForBuffered(K key);
        void EvictWhile(Func<bool> predicate, Action<K, T, IRecordContext> evictHandler);
    }
}