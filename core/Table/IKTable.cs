using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;
using System;
using System.Collections.Generic;
using System.Net.WebSockets;

namespace Streamiz.Kafka.Net.Table
{
    internal interface IKTableGetter<K, V>
    {
        IKTableValueGetterSupplier<K, V> ValueGetterSupplier { get; }
        void EnableSendingOldValues();
    }

    /// <summary>
    /// <see cref="IKTable{K, V}"/> is an abstraction of a changelog stream from a primary-keyed table.
    /// Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
    /// A <see cref="IKTable{K, V}"/> is either <see cref="StreamBuilder.Table{K, V}(string)"/> defined from a single Kafka topic that is consumed message by message or the result of a <see cref="IKTable{K, V}"/> transformation.
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
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when predicate function is null</exception>
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
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when predicate function is null</exception>
        IKTable<K, V> Filter(Func<K, V, bool> predicate, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

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
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when predicate function is null</exception>
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
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when predicate function is null</exception>
        IKTable<K, V> FilterNot(Func<K, V, bool> predicate, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

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

        /// <summary>
        /// Create a new <see cref="IKTable{K, V}"/> by transforming the value of each record in this <see cref="IKTable{K, V}"/> into a new value
        /// (with possibly a new type) in the new <see cref="IKTable{K, V}"/>, with default serializers, deserializers, and state store.
        /// For each <see cref="IKTable{K, V}"/> update the provided <see cref="IValueMapper{V, VR}"/> is applied to the value of the updated record and
        /// computes a new value for it, resulting in an updated record for the result <see cref="IKTable{K, V}"/>.
        /// Thus, an input record <code>&lt;K,V&gt;</code> can be transformed into an output record <code>&lt;K,VR&gt;</code>.
        /// This is a stateless record-by-record operation.
        /// The example below counts the number of token of the value string.
        /// <example>
        /// <code>
        /// var table = builder.Table&lt;string, string&gt;("topic");
        /// var output = table.MapValues((v) => v.Split(" ").Length);
        /// </code>
        /// </example>
        /// This operation preserves data co-location with respect to the key.
        /// Thus, NO internal data redistribution is required if a key based operator (like a join) is applied to the result <see cref="IKTable{K, V}"/>
        /// Note that <see cref="IKTable{K, V}.MapValues{VR}(Func{K, V, VR}, string)"/> for a changelog stream works differently than <see cref="IKStream{K, V}.MapValues{VR}(Func{K, V, VR}, string)"/>
        /// record stream filters}, because <see cref="KeyValuePair{K, V}"/> with null values (so-called tombstone records)
        /// have delete semantics.
        /// Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
        /// delete the corresponding record in the result <see cref="IKTable{K, VR}"/>
        /// </summary>
        /// <typeparam name="VR">the value type of the result <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="mapper">a function mapper that computes a new output value</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>a <see cref="IKTable{K, VR}"/> that contains records with unmodified keys and new values (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper, string named = null);

        /// <summary>
        /// Create a new <see cref="IKTable{K, V}"/> by transforming the value of each record in this <see cref="IKTable{K, V}"/> into a new value
        /// (with possibly a new type) in the new <see cref="IKTable{K, V}"/>, with the <see cref="ISerDes{K}"/> key serde, <see cref="ISerDes{V}"/> value serde},
        /// and the underlying IKeyValueStore materialized state storage configured in the <see cref="Materialized{K, V, S}"/>
        /// instance.
        /// For each <see cref="IKTable{K, V}"/> update the provided mapper is applied to the value of the updated record and
        /// computes a new value for it, resulting in an updated record for the result <see cref="IKTable{K, V}"/>.
        /// Thus, an input record <code>&lt;K,V&gt;</code> can be transformed into an output record <code>&lt;K,VR&gt;</code>.
        /// This is a stateless record-by-record operation.
        /// Note that <see cref="IKTable{K, V}.MapValues{VR}(Func{V, VR}, string)"/> for a changelog stream works differently than <see cref="IKStream{K, V}.MapValues{VR}(Func{V, VR}, string)"/>
        /// because keyvalue records with null values (so-called tombstone records)
        /// have delete semantics.
        /// Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
        /// delete the corresponding record in the result <see cref="IKTable{K, V}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="mapper">a function mapper that computes a new output value</param>
        /// <param name="materialized">A materialized that describes how the <see cref="IStateStore"/> for the resulting <see cref="IKTable{K, VR}"/> should be materialized.</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKTable{K, VR}"/> that contains records with unmodified keys and new values (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKTable<K, VR> MapValues<VR>(Func<V, VR> mapper, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Create a new <see cref="IKTable{K, V}"/> by transforming the value of each record in this <see cref="IKTable{K, V}"/> into a new value
        /// (with possibly a new type) in the new <see cref="IKTable{K, V}"/>, with the <see cref="ISerDes{K}"/> key serde, <see cref="ISerDes{V}"/> value serde},
        /// and the underlying IKeyValueStore materialized state storage configured in the <see cref="Materialized{K, V, S}"/>
        /// instance.
        /// For each <see cref="IKTable{K, V}"/> update the provided mapper is applied to the value of the updated record and
        /// computes a new value for it, resulting in an updated record for the result <see cref="IKTable{K, V}"/>.
        /// Thus, an input record <code>&lt;K,V&gt;</code> can be transformed into an output record <code>&lt;K,VR&gt;</code>.
        /// This is a stateless record-by-record operation.
        /// Note that <see cref="IKTable{K, V}.MapValues{VR}(Func{V, VR}, string)"/> for a changelog stream works differently than <see cref="IKStream{K, V}.MapValues{VR}(Func{V, VR}, string)"/>
        /// because keyvalue records with null values (so-called tombstone records)
        /// have delete semantics.
        /// Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
        /// delete the corresponding record in the result <see cref="IKTable{K, V}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="mapper">a <see cref="IValueMapper{V, VR}"/> mapper that computes a new output value</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKTable{K, VR}"/> that contains records with unmodified keys and new values (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, string named = null);

        /// <summary>
        /// Create a new <see cref="IKTable{K, V}"/> by transforming the value of each record in this <see cref="IKTable{K, V}"/> into a new value
        /// (with possibly a new type) in the new <see cref="IKTable{K, V}"/>, with the <see cref="ISerDes{K}"/> key serde, <see cref="ISerDes{V}"/> value serde},
        /// and the underlying IKeyValueStore materialized state storage configured in the <see cref="Materialized{K, V, S}"/>
        /// instance.
        /// For each <see cref="IKTable{K, V}"/> update the provided mapper is applied to the value of the updated record and
        /// computes a new value for it, resulting in an updated record for the result <see cref="IKTable{K, V}"/>.
        /// Thus, an input record <code>&lt;K,V&gt;</code> can be transformed into an output record <code>&lt;K,VR&gt;</code>.
        /// This is a stateless record-by-record operation.
        /// Note that <see cref="IKTable{K, V}.MapValues{VR}(IValueMapper{V, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> for a changelog stream works differently than <see cref="IKStream{K, V}.MapValues{VR}(IValueMapper{V, VR}, string)"/>
        /// because keyvalue records with null values (so-called tombstone records)
        /// have delete semantics.
        /// Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
        /// delete the corresponding record in the result <see cref="IKTable{K, V}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="mapper">a <see cref="IValueMapper{V, VR}"/> mapper that computes a new output value</param>
        /// <param name="materialized">A materialized that describes how the <see cref="IStateStore"/> for the resulting <see cref="IKTable{K, VR}"/> should be materialized.</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKTable{K, VR}"/> that contains records with unmodified keys and new values (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKTable<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Create a new <see cref="IKTable{K, V}"/> by transforming the value of each record in this <see cref="IKTable{K, V}"/> into a new value
        /// (with possibly a new type) in the new <see cref="IKTable{K, V}"/>, with the <see cref="ISerDes{K}"/> key serde, <see cref="ISerDes{V}"/> value serde},
        /// and the underlying IKeyValueStore materialized state storage configured in the <see cref="Materialized{K, V, S}"/>
        /// instance.
        /// For each <see cref="IKTable{K, V}"/> update the provided mapper is applied to the value of the updated record and
        /// computes a new value for it, resulting in an updated record for the result <see cref="IKTable{K, V}"/>.
        /// Thus, an input record <code>&lt;K,V&gt;</code> can be transformed into an output record <code>&lt;K,VR&gt;</code>.
        /// This is a stateless record-by-record operation.
        /// Note that <see cref="IKTable{K, V}.MapValues{VR}(IValueMapper{V, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> for a changelog stream works differently than <see cref="IKStream{K, V}.MapValues{VR}(IValueMapper{V, VR}, string)"/>
        /// because keyvalue records with null values (so-called tombstone records)
        /// have delete semantics.
        /// Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
        /// delete the corresponding record in the result <see cref="IKTable{K, V}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="mapperWithKey">a function mapper that computes a new output value (key is for readonly usage)</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKTable{K, VR}"/> that contains records with unmodified keys and new values (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKTable<K, VR> MapValues<VR>(Func<K, V, VR> mapperWithKey, string named = null);

        /// <summary>
        /// Create a new <see cref="IKTable{K, V}"/> by transforming the value of each record in this <see cref="IKTable{K, V}"/> into a new value
        /// (with possibly a new type) in the new <see cref="IKTable{K, V}"/>, with the <see cref="ISerDes{K}"/> key serde, <see cref="ISerDes{V}"/> value serde},
        /// and the underlying IKeyValueStore materialized state storage configured in the <see cref="Materialized{K, V, S}"/>
        /// instance.
        /// For each <see cref="IKTable{K, V}"/> update the provided mapper is applied to the value of the updated record and
        /// computes a new value for it, resulting in an updated record for the result <see cref="IKTable{K, V}"/>.
        /// Thus, an input record <code>&lt;K,V&gt;</code> can be transformed into an output record <code>&lt;K,VR&gt;</code>.
        /// This is a stateless record-by-record operation.
        /// Note that <see cref="IKTable{K, V}.MapValues{VR}(IValueMapper{V, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> for a changelog stream works differently than <see cref="IKStream{K, V}.MapValues{VR}(IValueMapper{V, VR}, string)"/>
        /// because keyvalue records with null values (so-called tombstone records)
        /// have delete semantics.
        /// Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
        /// delete the corresponding record in the result <see cref="IKTable{K, V}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="mapperWithKey">a function mapper that computes a new output value (key is for readonly usage)</param>
        /// <param name="materialized">A materialized that describes how the <see cref="IStateStore"/> for the resulting <see cref="IKTable{K, VR}"/> should be materialized.</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKTable{K, VR}"/> that contains records with unmodified keys and new values (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKTable<K, VR> MapValues<VR>(Func<K, V, VR> mapperWithKey, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Create a new <see cref="IKTable{K, V}"/> by transforming the value of each record in this <see cref="IKTable{K, V}"/> into a new value
        /// (with possibly a new type) in the new <see cref="IKTable{K, V}"/>, with the <see cref="ISerDes{K}"/> key serde, <see cref="ISerDes{V}"/> value serde},
        /// and the underlying IKeyValueStore materialized state storage configured in the <see cref="Materialized{K, V, S}"/>
        /// instance.
        /// For each <see cref="IKTable{K, V}"/> update the provided mapper is applied to the value of the updated record and
        /// computes a new value for it, resulting in an updated record for the result <see cref="IKTable{K, V}"/>.
        /// Thus, an input record <code>&lt;K,V&gt;</code> can be transformed into an output record <code>&lt;K,VR&gt;</code>.
        /// This is a stateless record-by-record operation.
        /// Note that <see cref="IKTable{K, V}.MapValues{VR}(IValueMapper{V, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> for a changelog stream works differently than <see cref="IKStream{K, V}.MapValues{VR}(IValueMapper{V, VR}, string)"/>
        /// because keyvalue records with null values (so-called tombstone records)
        /// have delete semantics.
        /// Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
        /// delete the corresponding record in the result <see cref="IKTable{K, V}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="mapperWithKey">a <see cref="IValueMapperWithKey{K, V, VR}"/> mapper that computes a new output value (key is for readonly usage)</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKTable{K, VR}"/> that contains records with unmodified keys and new values (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, string named = null);

        /// <summary>
        /// Create a new <see cref="IKTable{K, V}"/> by transforming the value of each record in this <see cref="IKTable{K, V}"/> into a new value
        /// (with possibly a new type) in the new <see cref="IKTable{K, V}"/>, with the <see cref="ISerDes{K}"/> key serde, <see cref="ISerDes{V}"/> value serde},
        /// and the underlying IKeyValueStore materialized state storage configured in the <see cref="Materialized{K, V, S}"/>
        /// instance.
        /// For each <see cref="IKTable{K, V}"/> update the provided mapper is applied to the value of the updated record and
        /// computes a new value for it, resulting in an updated record for the result <see cref="IKTable{K, V}"/>.
        /// Thus, an input record <code>&lt;K,V&gt;</code> can be transformed into an output record <code>&lt;K,VR&gt;</code>.
        /// This is a stateless record-by-record operation.
        /// Note that <see cref="IKTable{K, V}.MapValues{VR}(IValueMapper{V, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> for a changelog stream works differently than <see cref="IKStream{K, V}.MapValues{VR}(IValueMapper{V, VR}, string)"/>
        /// because keyvalue records with null values (so-called tombstone records)
        /// have delete semantics.
        /// Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
        /// delete the corresponding record in the result <see cref="IKTable{K, V}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="mapperWithKey">a <see cref="IValueMapperWithKey{K, V, VR}"/> mapper that computes a new output value (key is for readonly usage)</param>
        /// <param name="materialized">A materialized that describes how the <see cref="IStateStore"/> for the resulting <see cref="IKTable{K, VR}"/> should be materialized.</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        /// <returns>A <see cref="IKTable{K, VR}"/> that contains records with unmodified keys and new values (possibly of different type)</returns>
        IKTable<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapperWithKey, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Re-groups the records of this <see cref="IKTable{K, V}"/> using the provided <see cref="IKeyValueMapper{K, V, VR}"/> and default serializers
        /// and deserializers.
        /// Each keyvlaue pair of this <see cref="IKTable{K, V}"/> is mapped to a new keyvalue pair by applying the
        /// provided <see cref="IKeyValueMapper{K, V, VR}"/>.
        /// Re-grouping a <see cref="IKTable{K, V}"/> is required before an aggregation operator can be applied to the data.
        /// The <see cref="IKeyValueMapper{K, V, VR}"/> mapper selects a new key and value (with should both have unmodified type).
        /// If the new record key is null the record will not be included in the resulting <see cref="IKGroupedTable{KR, VR}"/>
        /// <p>
        /// Because a new key is selected, an internal repartitioning topic will be created in Kafka.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>, "&lt;name&gt;" is
        /// an internally generated name, and "-repartition" is a fixed suffix.
        /// All data of this <see cref="IKTable{K, V}"/> will be redistributed through the repartitioning topic by writing all update
        /// records to and rereading all updated records from it, such that the resulting <see cref="IKGroupedTable{KR, VR}"/> is partitioned
        /// on the new key.
        /// </p>
        /// If the key or value type is changed, it is recommended to use <see cref="IKTable{K, V}.GroupBy{KR, VR, KRS, VRS}(IKeyValueMapper{K, V, KeyValuePair{KR, VR}}, string)"/>
        /// instead.
        /// </summary>
        /// <typeparam name="KR">the key type of the result <see cref="IKGroupedTable{KR, VR}"/></typeparam>
        /// <typeparam name="VR">the value type of the result <see cref="IKGroupedTable{KR, VR}"/></typeparam>
        /// <param name="keySelector">a <see cref="IKeyValueMapper{K, V, VR}"/> mapper that computes a new grouping key and value to be aggregated</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>a <see cref="IKGroupedTable{KR, VR}"/> that contains the re-grouped records of the original <see cref="IKTable{K, V}"/></returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when selector function is null</exception>
        IKGroupedTable<KR, VR> GroupBy<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> keySelector, string named = null);

        /// <summary>
        /// Re-groups the records of this <see cref="IKTable{K, V}"/> using the provided <code>Func&lt;K, V, KeyValuePair&lt;KR,VR&gt;&gt;</code> and default serializers
        /// and deserializers.
        /// Each keyvlaue pair of this <see cref="IKTable{K, V}"/> is mapped to a new keyvalue pair by applying the
        /// provided <code>Func&lt;K, V, KeyValuePair&lt;KR,VR&gt;&gt;</code>.
        /// Re-grouping a <see cref="IKTable{K, V}"/> is required before an aggregation operator can be applied to the data.
        /// The<code>Func&lt;K, V, KeyValuePair&lt;KR,VR&gt;&gt;</code> mapper selects a new key and value (with should both have unmodified type).
        /// If the new record key is null the record will not be included in the resulting <see cref="IKGroupedTable{KR, VR}"/>
        /// <p>
        /// Because a new key is selected, an internal repartitioning topic will be created in Kafka.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>, "&lt;name&gt;" is
        /// an internally generated name, and "-repartition" is a fixed suffix.
        /// All data of this <see cref="IKTable{K, V}"/> will be redistributed through the repartitioning topic by writing all update
        /// records to and rereading all updated records from it, such that the resulting <see cref="IKGroupedTable{KR, VR}"/> is partitioned
        /// on the new key.
        /// </p>
        /// If the key or value type is changed, it is recommended to use <see cref="IKTable{K, V}.GroupBy{KR, VR, KRS, VRS}(Func{K, V, KeyValuePair{KR, VR}}, string)"/>
        /// instead.
        /// </summary>
        /// <typeparam name="KR">the key type of the result <see cref="IKGroupedTable{KR, VR}"/></typeparam>
        /// <typeparam name="VR">the value type of the result <see cref="IKGroupedTable{KR, VR}"/></typeparam>
        /// <param name="keySelector">a function mapper that computes a new grouping key and value to be aggregated</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>a <see cref="IKGroupedTable{KR, VR}"/> that contains the re-grouped records of the original <see cref="IKTable{K, V}"/></returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when selector function is null</exception>
        IKGroupedTable<KR, VR> GroupBy<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> keySelector, string named = null);

        /// <summary>
        /// Re-groups the records of this <see cref="IKTable{K, V}"/> using the provided <see cref="IKeyValueMapper{K, V, VR}"/> and serdes parameters from (<typeparamref name="KR"/> as key serdes,
        /// and <typeparamref name="VRS"/> as value serdes) 
        /// Each keyvlaue pair of this <see cref="IKTable{K, V}"/> is mapped to a new keyvalue pair by applying the
        /// provided <see cref="IKeyValueMapper{K, V, VR}"/>.
        /// Re-grouping a <see cref="IKTable{K, V}"/> is required before an aggregation operator can be applied to the data.
        /// The <see cref="IKeyValueMapper{K, V, VR}"/> mapper selects a new key and value (with should both have unmodified type).
        /// If the new record key is null the record will not be included in the resulting <see cref="IKGroupedTable{KR, VR}"/>
        /// <p>
        /// Because a new key is selected, an internal repartitioning topic will be created in Kafka.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>, "&lt;name&gt;" is
        /// an internally generated name, and "-repartition" is a fixed suffix.
        /// All data of this <see cref="IKTable{K, V}"/> will be redistributed through the repartitioning topic by writing all update
        /// records to and rereading all updated records from it, such that the resulting <see cref="IKGroupedTable{KR, VR}"/> is partitioned
        /// on the new key.
        /// </p>
        /// If the key or value type is changed, it is recommended to use <see cref="IKTable{K, V}.GroupBy{KR, VR, KRS, VRS}(IKeyValueMapper{K, V, KeyValuePair{KR, VR}}, string)"/>
        /// instead.
        /// </summary>
        /// <typeparam name="KR">the key type of the result <see cref="IKGroupedTable{KR, VR}"/></typeparam>
        /// <typeparam name="VR">the value type of the result <see cref="IKGroupedTable{KR, VR}"/></typeparam>
        /// <typeparam name="KRS">New serializer for <typeparamref name="KR"/> type</typeparam>
        /// <typeparam name="VRS">New serializer for <typeparamref name="VR"/> type</typeparam>
        /// <param name="keySelector">a <see cref="IKeyValueMapper{K, V, VR}"/> mapper that computes a new grouping key and value to be aggregated</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>a <see cref="IKGroupedTable{KR, VR}"/> that contains the re-grouped records of the original <see cref="IKTable{K, V}"/></returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when selector function is null</exception>
        IKGroupedTable<KR, VR> GroupBy<KR, VR, KRS, VRS>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> keySelector, string named = null) where KRS : ISerDes<KR>, new() where VRS : ISerDes<VR>, new();

        /// <summary>
        /// Re-groups the records of this <see cref="IKTable{K, V}"/> using the provided <see cref="IKeyValueMapper{K, V, VR}"/> and serdes parameters from (<typeparamref name="KR"/> as key serdes,
        /// and <typeparamref name="VRS"/> as value serdes) 
        /// Each keyvlaue pair of this <see cref="IKTable{K, V}"/> is mapped to a new keyvalue pair by applying the
        /// provided <code>Func&lt;K, V, KeyValuePair&lt;KR,VR&gt;&gt;</code>.
        /// Re-grouping a <see cref="IKTable{K, V}"/> is required before an aggregation operator can be applied to the data.
        /// The<code>Func&lt;K, V, KeyValuePair&lt;KR,VR&gt;&gt;</code> mapper selects a new key and value (with should both have unmodified type).
        /// If the new record key is null the record will not be included in the resulting <see cref="IKGroupedTable{KR, VR}"/>
        /// <p>
        /// Because a new key is selected, an internal repartitioning topic will be created in Kafka.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>, "&lt;name&gt;" is
        /// an internally generated name, and "-repartition" is a fixed suffix.
        /// All data of this <see cref="IKTable{K, V}"/> will be redistributed through the repartitioning topic by writing all update
        /// records to and rereading all updated records from it, such that the resulting <see cref="IKGroupedTable{KR, VR}"/> is partitioned
        /// on the new key.
        /// </p>
        /// If the key or value type is changed, it is recommended to use <see cref="IKTable{K, V}.GroupBy{KR, VR, KRS, VRS}(Func{K, V, KeyValuePair{KR, VR}}, string)"/>
        /// instead.
        /// </summary>
        /// <typeparam name="KR">the key type of the result <see cref="IKGroupedTable{KR, VR}"/></typeparam>
        /// <typeparam name="VR">the value type of the result <see cref="IKGroupedTable{KR, VR}"/></typeparam>
        /// <typeparam name="KRS">New serializer for <typeparamref name="KR"/> type</typeparam>
        /// <typeparam name="VRS">New serializer for <typeparamref name="VR"/> type</typeparam>
        /// <param name="keySelector">a function mapper that computes a new grouping key and value to be aggregated</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>a <see cref="IKGroupedTable{KR, VR}"/> that contains the re-grouped records of the original <see cref="IKTable{K, V}"/></returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when selector function is null</exception>
        IKGroupedTable<KR, VR> GroupBy<KR, VR, KRS, VRS>(Func<K, V, KeyValuePair<KR, VR>> keySelector, string named = null) where KRS : ISerDes<KR>, new() where VRS : ISerDes<VR>, new();

        IKTable<K, VR> Join<VT, VR>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner);
        IKTable<K, VR> Join<VT, VR>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);
        IKTable<K, VR> Join<VT, VR>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner);
        IKTable<K, VR> Join<VT, VR>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);
        //IKTable<K, VR> Join<VT, VR, VTS, VRS>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner);
        //IKTable<K, VR> Join<VT, VR, VTS, VRS>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);
        //IKTable<K, VR> Join<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner);
        //IKTable<K, VR> Join<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);
        //IKTable<K, VR> LeftJoin<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner);
        //IKTable<K, VR> LeftJoin<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);
        //IKTable<K, VR> OuterJoin<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner);
        //IKTable<K, VR> OuterJoin<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);
    }
}