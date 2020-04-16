using Kafka.Streams.Net.Crosscutting;
using Kafka.Streams.Net.Processors;
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

    /// <summary>
    /// <see cref="IKTable{K, V}"/> is an abstraction of a changelog stream from a primary-keyed table.
    /// Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
    /// A <see cref="IKTable{K, V}"/> is either <see cref="StreamBuilder.Table(string, StreamOptions)"/> defined from a single Kafka topic that is consumed message by message or the result of a <see cref="IKTable{K, V}"/> transformation.
    /// An aggregation of a <see cref="IKStream{K, V}"/> also yields a <see cref="IKTable{K, V}"/>.
    /// A <see cref="IKTable{K, V}"/> can be transformed record by record, joined with another <see cref="IKTable{K, V}"/> or <see cref="IKStream{K, V}"/>, or
    /// can be re-partitioned and aggregated into a new <see cref="IKTable{K, V}"/>.
    /// Some <see cref="IKTable{K, V}"/> have an internal state <see cref="ReadOnlyKeyValueStore{K, V}"/> and are therefore queryable via the interactive queries API.
    /// Records from the source topic that have null keys are dropped.
    /// </summary>
    /// <typeparam name="K">Type of primary key</typeparam>
    /// <typeparam name="V">Type of value changes</typeparam>
    public interface IKTable<K, V>
    {
        /// <summary>
        /// Create a new <see cref="IKTable{K, V}"/> that consists of all records of this <see cref="IKTable{K, V}"/> which satisfy the given
        /// predicate, with default serializers, deserializers, and state store.
        /// All records that do not satisfy the predicate are dropped.
        /// For each <see cref="IKTable{K, V}"/> update, the filter is evaluated based on the current update
        /// record and then an update record is produced for the result <see cref="IKTable{K, V}"/>.
        /// This is a stateless record-by-record operation.
        /// Note that filter for a changelog stream works differently than <see cref="IKStream{K, V}.Filter(Func{K, V, bool}, string)"/>
        /// record stream filters, because <see cref="KeyValuePair{K, V}"/> with null values (so-called tombstone records)
        /// have delete semantics.
        /// Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
        /// directly if required (i.e., if there is anything to be deleted).
        /// Furthermore, for each record that gets dropped (i.e., does not satisfy the given predicate) a tombstone record
        /// is forwarded.
        /// </summary>
        /// <param name="predicate">A filter that is applied to each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKTable{K, V}"/> that contains only those records that satisfy the given predicate</returns>
        IKTable<K, V> Filter(Func<K, V, bool> predicate, string named = null);

        /// <summary>
        /// Create a new <see cref="IKTable{K, V}"/> that consists of all records of this <see cref="IKTable{K, V}"/> which satisfy the given
        /// predicate, with default serializers, deserializers, and state store.
        /// All records that do not satisfy the predicate are dropped.
        /// For each <see cref="IKTable{K, V}"/> update, the filter is evaluated based on the current update
        /// record and then an update record is produced for the result <see cref="IKTable{K, V}"/>.
        /// This is a stateless record-by-record operation.
        /// Note that filter for a changelog stream works differently than <see cref="IKStream{K, V}.Filter(Func{K, V, bool}, string)"/>
        /// record stream filters, because <see cref="KeyValuePair{K, V}"/> with null values (so-called tombstone records)
        /// have delete semantics.
        /// Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
        /// directly if required (i.e., if there is anything to be deleted).
        /// Furthermore, for each record that gets dropped (i.e., does not satisfy the given predicate) a tombstone record
        /// is forwarded.
        /// </summary>
        /// <param name="predicate">A filter that is applied to each record</param>
        /// <param name="materialized">A <see cref="Materialized{K, V, S}"/> that describes how the <see cref="IStateStore"/> for the resulting <see cref="IKTable{K, V}"/> should be materialized.</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKTable{K, V}"/> that contains only those records that satisfy the given predicate</returns>
        IKTable<K, V> Filter(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Create a new <see cref="IKTable{K, V}"/> that consists of all records of this <see cref="IKTable{K, V}"/> which DO NOT satisfy the given
        /// predicate, with default serializers, deserializers, and state store.
        /// All records that do not satisfy the predicate are dropped.
        /// For each <see cref="IKTable{K, V}"/> update, the filter is evaluated based on the current update
        /// record and then an update record is produced for the result <see cref="IKTable{K, V}"/>.
        /// This is a stateless record-by-record operation.
        /// Note that filter for a changelog stream works differently than <see cref="IKStream{K, V}.FilterNot(Func{K, V, bool}, string)"/>
        /// record stream filters, because <see cref="KeyValuePair{K, V}"/> with null values (so-called tombstone records)
        /// have delete semantics.
        /// Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
        /// directly if required (i.e., if there is anything to be deleted).
        /// Furthermore, for each record that gets dropped (i.e., does not satisfy the given predicate) a tombstone record
        /// is forwarded.
        /// </summary>
        /// <param name="predicate">A filter that is applied to each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKTable{K, V}"/> that contains only those records that satisfy the given predicate</returns>
        IKTable<K, V> FilterNot(Func<K, V, bool> predicate, string named = null);

        /// <summary>
        /// Create a new <see cref="IKTable{K, V}"/> that consists of all records of this <see cref="IKTable{K, V}"/> which DO NOT satisfy the given
        /// predicate, with default serializers, deserializers, and state store.
        /// All records that do not satisfy the predicate are dropped.
        /// For each <see cref="IKTable{K, V}"/> update, the filter is evaluated based on the current update
        /// record and then an update record is produced for the result <see cref="IKTable{K, V}"/>.
        /// This is a stateless record-by-record operation.
        /// Note that filter for a changelog stream works differently than <see cref="IKStream{K, V}.Filter(Func{K, V, bool}, string)"/>
        /// record stream filters, because <see cref="KeyValuePair{K, V}"/> with null values (so-called tombstone records)
        /// have delete semantics.
        /// Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
        /// directly if required (i.e., if there is anything to be deleted).
        /// Furthermore, for each record that gets dropped (i.e., does not satisfy the given predicate) a tombstone record
        /// is forwarded.
        /// </summary>
        /// <param name="predicate">A filter that is applied to each record</param>
        /// <param name="materialized">A <see cref="Materialized{K, V, S}"/> that describes how the <see cref="IStateStore"/> for the resulting <see cref="IKTable{K, V}"/> should be materialized.</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKTable{K, V}"/> that contains only those records that satisfy the given predicate</returns>
        IKTable<K, V> FilterNot(Func<K, V, bool> predicate, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Convert this changelog stream to a <see cref="IKStream{K, V}"/>.
        /// Note that this is a logical operation and only changes the "interpretation" of the stream, i.e., each record of
        /// this changelog stream is no longer treated as an updated record (cf. <see cref="IKStream{K, V}"/> vs {@code <see cref="IKTable{K, V}"/>).
        /// </summary>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, V}"/> that contains the same records as this <see cref="IKTable{K, V}"/></returns>
        IKStream<K, V> ToStream(string named = null);

        /// <summary>
        /// Convert this changelog stream to a <see cref="IKStream{K, V}"/> using the given <see cref="IKeyValueMapper{K, V, VR}" /> to select the new key.
        /// Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
        /// join) is applied to the result <see cref="IKStream{KR, V}"/>.
        /// This operation is equivalent to calling <see cref="IKTable{K, V}.ToStream(string)"/>.<see cref="IKStream{K, V}.SelectKey{KR}(Func{K, V, KR}, string)"/>
        /// Note that <see cref="IKTable{K, V}.ToStream{KR}(Func{K, V, KR}, string)"/> is a logical operation and only changes the "interpretation" of the stream, i.e.,
        /// each record of this changelog stream is no longer treated as an updated record (cf. <see cref="IKStream{K, V}"/> vs {@code <see cref="IKTable{K, V}"/>).
        /// </summary>
        /// <typeparam name="KR">the new key type of the result stream</typeparam>
        /// <param name="mapper">a <see cref="IKeyValueMapper{K, V, VR}"/> mapper that computes a new key for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>a <see cref="IKStream{KR, V}"/> that contains the same records as this <see cref="IKTable{K, V}"/></returns>
        IKStream<KR, V> ToStream<KR>(IKeyValueMapper<K, V, KR> mapper, string named = null);

        /// <summary>
        /// Convert this changelog stream to a <see cref="IKStream{K, V}"/> using the given <code>Func&lt;K, V, KR&gt;</code> to select the new key.
        /// For example, you can compute the new key as the length of the value string.
        /// <example>
        /// <code>
        /// var table = builder.Table&lt;string, string&gt;("topic");
        /// var stream = table.ToStream((k,v) => v.Length);
        /// </code>
        /// </example>
        /// Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
        /// join) is applied to the result <see cref="IKStream{KR, V}"/>.
        /// This operation is equivalent to calling <see cref="IKTable{K, V}.ToStream(string)"/>.<see cref="IKStream{K, V}.SelectKey{KR}(Func{K, V, KR}, string)"/>
        /// Note that <see cref="IKTable{K, V}.ToStream{KR}(Func{K, V, KR}, string)"/> is a logical operation and only changes the "interpretation" of the stream, i.e.,
        /// each record of this changelog stream is no longer treated as an updated record (cf. <see cref="IKStream{K, V}"/> vs {@code <see cref="IKTable{K, V}"/>).
        /// </summary>
        /// <typeparam name="KR">the new key type of the result stream</typeparam>
        /// <param name="mapper">a function mapper that computes a new key for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>a <see cref="IKStream{KR, V}"/> that contains the same records as this <see cref="IKTable{K, V}"/></returns>
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