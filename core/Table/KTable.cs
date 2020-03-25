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
        //KStream<KR, V> toStream<KR>(IKeyValueMapper<K, V, KR> mapper);
        //KStream<KR, V> toStream<KR>(IKeyValueMapper<K, V, KR>  mapper, string named);
        //KTable<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper);
        //KTable<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper, string name);
        //KTable<K, VR> mapValues<VR>(Func<V, VR> mapper, string name);
        //KTable<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey);
        //KTable<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, string name);
        //KTable<K, VR> mapValues<VR>(Func<K, V, VR> mapperWithKey, string name);
        //KTable<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper, Materialized<K, VR, KeyValueStore<byte[], byte[]>> materialized);
        //KTable<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper, string name, Materialized<K, VR, KeyValueStore<byte[], byte[]>> materialized);
        //KTable<K, VR> mapValues<VR>(Func<V, VR> mapper, string name, Materialized<K, VR, KeyValueStore<byte[], byte[]>> materialized);
        //KTable<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, Materialized<K, VR, KeyValueStore<byte[], byte[]>> materialized);
        //KTable<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, string name, Materialized<K, VR, KeyValueStore<byte[], byte[]>> materialized);
        //KTable<K, VR> mapValues<VR>(Func<K, V, VR> mapperWithKey, string name, Materialized<K, VR, KeyValueStore<byte[], byte[]>> materialized);
    }
}
