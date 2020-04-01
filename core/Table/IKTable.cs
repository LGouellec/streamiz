using kafka_stream_core.Crosscutting;
using kafka_stream_core.State;
using kafka_stream_core.Stream;
using kafka_stream_core.Table.Internal;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Table
{
    internal interface IKTableGetter<K, V>
    {
        IKTableValueGetterSupplier<K, V> ValueGetterSupplier { get; }
        void EnableSendingOldValues();
    }

    public interface IKTable<K, V>
    {
        IKTable<K, V> Filter(Func<K, V, bool> predicate);
        IKTable<K, V> Filter(Func<K, V, bool> predicate, string named);
        IKTable<K, V> Filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);
        IKTable<K, V> Filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized, string named);
        IKTable<K, V> FilterNot(Func<K, V, bool> predicate);
        IKTable<K, V> FilterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);
        IKTable<K, V> FilterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized, string named);
        IKStream<K, V> ToStream();
        IKStream<K, V> ToStream(string named);
        IKStream<KR, V> ToStream<KR>(IKeyValueMapper<K, V, KR> mapper);
        IKStream<KR, V> ToStream<KR>(IKeyValueMapper<K, V, KR> mapper, string named);
        IKStream<KR, V> ToStream<KR>(Func<K, V, KR> mapper);
        IKStream<KR, V> ToStream<KR>(Func<K, V, KR> mapper, string named);
        IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper);
        IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper, string name);
        IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper, string name, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);
        IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, string name);
        IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper);
        IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, string name);
        IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);
        IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, string name, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);
        IKTable<K, VR> MapValues<VR>(Func<K, V, VR> mapperWithKey);
        IKTable<K, VR> MapValues<VR>(Func<K, V, VR> mapperWithKey, string name);
        IKTable<K, VR> MapValues<VR>(Func<K, V, VR> mapperWithKey, string name, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);
        IKTable<K, VR> MapValues<VR>(Func<K, V, VR> mapperWithKey, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, string name);
        IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey);
        IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, string name);
        IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);
        IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, string name, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);
        IKGroupedTable<K1, V1> GroupBy<K1, V1>(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> keySelector);
        IKGroupedTable<K1, V1> GroupBy<K1, V1>(Func<K, V, KeyValuePair<K1, V1>> keySelector);
        IKGroupedTable<K1, V1> GroupBy<K1, V1>(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> keySelector, Grouped<K1, V1> grouped);
        IKGroupedTable<K1, V1> GroupBy<K1, V1>(Func<K, V, KeyValuePair<K1, V1>> keySelector, Grouped<K1, V1> grouped);
    }
}
