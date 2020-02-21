using kafka_stream_core.State;
using kafka_stream_core.Stream;
using System;

namespace kafka_stream_core.Table
{
    public interface KTable<K, V>
    {
        KTable<K, V> filter(Func<K, V, bool> predicate);
        KTable<K, V> filter(Func<K, V, bool> predicate, string named);
        KTable<K, V> filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized);
        KTable<K, V> filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized, string named);
        KTable<K, V> filterNot(Func<K, V, bool> predicate);
        KTable<K, V> filterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized);
        KTable<K, V> filterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<byte[], byte[]>> materialized, string named);
        KStream<K, V> toStream();
        KStream<K, V> toStream(string named);
        void @foreach(Action<K, V> action);
        void @foreach(Action<K, V> action, string named);
    }
}
