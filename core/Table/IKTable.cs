using Kafka.Streams.Net.Crosscutting;
using Kafka.Streams.Net.SerDes;
using Kafka.Streams.Net.State;
using Kafka.Streams.Net.Stream;
using Kafka.Streams.Net.Table.Internal;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Net.Table
{
    internal interface IKTableGetter<K, V>
    {
        IKTableValueGetterSupplier<K, V> ValueGetterSupplier { get; }
        void EnableSendingOldValues();
    }

    public interface IKTable<K, V>
    {
        IKTable<K, V> Filter(Func<K, V, bool> predicate, string named = null);
        IKTable<K, V> Filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized, string named = null);
        IKTable<K, V> FilterNot(Func<K, V, bool> predicate, string named = null);
        IKTable<K, V> FilterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized, string named = null);
        IKStream<K, V> ToStream(string named = null);
        IKStream<KR, V> ToStream<KR>(IKeyValueMapper<K, V, KR> mapper, string named = null);
        IKStream<KR, V> ToStream<KR>(Func<K, V, KR> mapper, string named = null);
        IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper, string name = null);
        IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, string named = null);
        IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, string named = null);
        IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, string named = null);
        IKTable<K, VR> MapValues<VR>(Func<K, V, VR> mapperWithKey, string named = null);
        IKTable<K, VR> MapValues<VR>(Func<K, V, VR> mapperWithKey, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, string named = null);
        IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, string named = null);
        IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized, string named = null);
        IKGroupedTable<KR, VR> GroupBy<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> keySelector, string named = null);
        IKGroupedTable<KR, VR> GroupBy<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> keySelector, string named = null);
        IKGroupedTable<KR, VR> GroupBy<KR, VR, KRS, VRS>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> keySelector, string named = null) where KRS : ISerDes<KR>, new() where VRS : ISerDes<VR>, new();
        IKGroupedTable<KR, VR> GroupBy<KR, VR, KRS, VRS>(Func<K, V, KeyValuePair<KR, VR>> keySelector, string named = null) where KRS : ISerDes<KR>, new() where VRS : ISerDes<VR>, new();
    }
}