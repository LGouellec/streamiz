using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;
using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// <see cref="IKGroupedStream{K, V}"/> is an abstraction of a grouped record stream of keyvalue pairs.
    /// It is an intermediate representation of a <see cref="IKStream{K, V}"/> in order to apply an aggregation operation on the original
    /// <see cref="IKStream{K, V}"/> records.
    /// A <see cref="IKGroupedStream{K, V}"/> must be obtained from a <see cref="IKStream{K, V}"/> via <see cref="IKStream{K, V}.GroupByKey(string)"/> or
    /// <see cref="IKStream{K, V}.GroupBy{KR, KRS}(System.Func{K, V, KR}, string)" />
    /// </summary>
    /// <typeparam name="K">Type of key</typeparam>
    /// <typeparam name="V">Type of value</typeparam>
    public interface IKGroupedStream<K, V>
    {
        /// <summary>
        /// Count the number of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view).
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}" /> changelog stream.
        /// </summary>
        /// <returns>a <see cref="IKTable{K, V}" /> that contains "update" records with unmodified keys and <see cref="long" /> values that represent the latest (rolling) count (i.e., number of records) for each key</returns>
        IKTable<K, long> Count();

        /// <summary>
        /// Count the number of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view).
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}" /> changelog stream.
        /// </summary>
        /// <param name="named">a config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKTable{K, V}" /> that contains "update" records with unmodified keys and <see cref="long" /> values that represent the latest (rolling) count (i.e., number of records) for each key</returns>
        IKTable<K, long> Count(string named);

        /// <summary>
        /// Count the number of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// provided by the given store name in <see cref="Materialized{K, V, S}" />
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}" /> changelog stream.
        /// <p>
        /// Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
        /// the same key.
        /// </p>
        /// To query the local <see cref="IKeyValueStore{K, V}"/> it must be obtained via
        /// <see cref="KafkaStream.Store{T, K, V}(StoreQueryParameters{T, K, V})" />.
        /// </summary>
        /// <param name="materialized">an instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store. Node: the valueSerde will be automatically set to <see cref="Int64SerDes"/> if there is no valueSerde provided </param>
        /// <param name="named">a named config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys and <see cref="long"/> values that represent the latest (rolling) count (i.e., number of records) for each key</returns>
        IKTable<K, long> Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Aggregate the values of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// Aggregating is a generalization of Reducer combining via reduce(...)} as it, for example,
        /// allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IKeyValueStore{K, VR}"/> (which is basically an ever-updating materialized view)
        /// that can be queried by the given store name in {@code materialized}.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, VR}" /> changelog stream.
        /// <p>
        /// The specified <see cref="Func{VR}"/> initializer is applied once directly before the first input record is processed to
        /// provide an initial intermediate aggregation result that is used to process the first record.
        /// The specified <see cref="Func{K, V, VR, VR}"/> aggregator is applied for each input record and computes a new aggregate using the current
        /// aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// initialize) and the record's value.
        /// Thus, <see cref="Aggregate{VR, VRS}(Func{VR}, Func{K, V, VR, VR})"/> can be used to compute aggregate functions like
        /// count (c.f. <see cref="Count(Materialized{K, long, IKeyValueStore{Bytes, byte[]}}, string)"/>).
        /// </p>
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an <see cref="Func{VR}"/> initializer that computes an initial intermediate aggregation result</param>
        /// <param name="aggregator">an <see cref="Func{K, V, VR, VR}"/> aggregator that computes a new aggregate result</param>
        /// <returns>a <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the
        /// latest (rolling) aggregate for each key. If the aggregate function returns {@code null}, it is then interpreted as
        /// deletion for the key, and future messages of the same key coming from upstream operators
        /// will be handled as newly initialized value.</returns>
        IKTable<K, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator);

        /// <summary>
        /// Aggregate the values of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// Aggregating is a generalization of Reducer combining via reduce(...)} as it, for example,
        /// allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IKeyValueStore{K, VR}"/> (which is basically an ever-updating materialized view)
        /// that can be queried by the given store name in {@code materialized}.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, VR}" /> changelog stream.
        /// <p>
        /// The specified <see cref="Initializer{VR}"/> is applied once directly before the first input record is processed to
        /// provide an initial intermediate aggregation result that is used to process the first record.
        /// The specified <see cref="Aggregator{K, V, VR}"/> is applied for each input record and computes a new aggregate using the current
        /// aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// <see cref="Initializer{VR}"/>) and the record's value.
        /// Thus, <see cref="Aggregate{VR}(Initializer{VR}, Aggregator{K, V, VR})"/> can be used to compute aggregate functions like
        /// count (c.f. <see cref="Count(Materialized{K, long, IKeyValueStore{Bytes, byte[]}}, string)"/>).
        /// </p>
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an <see cref="Initializer{VR}"/>  that computes an initial intermediate aggregation result</param>
        /// <param name="aggregator">an <see cref="Aggregator{K, V, VR}"/> that computes a new aggregate result</param>
        /// <returns>a <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the
        /// latest (rolling) aggregate for each key. If the aggregate function returns {@code null}, it is then interpreted as
        /// deletion for the key, and future messages of the same key coming from upstream operators
        /// will be handled as newly initialized value.</returns>
        IKTable<K, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator);

        /// <summary>
        /// Aggregate the values of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// Aggregating is a generalization of Reducer combining via reduce(...)} as it, for example,
        /// allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IKeyValueStore{K, VR}"/> (which is basically an ever-updating materialized view)
        /// that can be queried by the given store name in {@code materialized}.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, VR}" /> changelog stream.
        /// <p>
        /// The specified <see cref="Func{VR}"/> initializer is applied once directly before the first input record is processed to
        /// provide an initial intermediate aggregation result that is used to process the first record.
        /// The specified <see cref="Func{K, V, VR, VR}"/> aggregator is applied for each input record and computes a new aggregate using the current
        /// aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// initialize) and the record's value.
        /// Thus, <see cref="Aggregate{VR, VRS}(Func{VR}, Func{K, V, VR, VR})"/> can be used to compute aggregate functions like
        /// count (c.f. <see cref="Count(Materialized{K, long, IKeyValueStore{Bytes, byte[]}}, string)"/>).
        /// </p>
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, VR}"/></typeparam>
        /// <typeparam name="VRS">New value serdes type</typeparam>
        /// <param name="initializer">an <see cref="Func{VR}"/> initializer that computes an initial intermediate aggregation result</param>
        /// <param name="aggregator">an <see cref="Func{K, V, VR, VR}"/> aggregator that computes a new aggregate result</param>
        /// <returns>a <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the
        /// latest (rolling) aggregate for each key. If the aggregate function returns {@code null}, it is then interpreted as
        /// deletion for the key, and future messages of the same key coming from upstream operators
        /// will be handled as newly initialized value.</returns>
        IKTable<K, VR> Aggregate<VR, VRS>(Func<VR> initializer, Func<K, V, VR, VR> aggregator) where VRS : ISerDes<VR>, new();

        /// <summary>
        /// Aggregate the values of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// Aggregating is a generalization of Reducer combining via reduce(...)} as it, for example,
        /// allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IKeyValueStore{K, VR}"/> (which is basically an ever-updating materialized view)
        /// that can be queried by the given store name in {@code materialized}.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, VR}" /> changelog stream.
        /// <p>
        /// The specified <see cref="Initializer{VR}"/> is applied once directly before the first input record is processed to
        /// provide an initial intermediate aggregation result that is used to process the first record.
        /// The specified <see cref="Aggregator{K, V, VR}"/> is applied for each input record and computes a new aggregate using the current
        /// aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// <see cref="Initializer{VR}"/>) and the record's value.
        /// Thus, <see cref="Aggregate{VR, VRS}(Initializer{VR}, Aggregator{K, V, VR})"/> can be used to compute aggregate functions like
        /// count (c.f. <see cref="Count(Materialized{K, long, IKeyValueStore{Bytes, byte[]}}, string)"/>).
        /// </p>
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, VR}"/></typeparam>
        /// <typeparam name="VRS">New value serdes type</typeparam>
        /// <param name="initializer">an <see cref="Initializer{VR}"/>  that computes an initial intermediate aggregation result</param>
        /// <param name="aggregator">an <see cref="Aggregator{K, V, VR}"/> that computes a new aggregate result</param>
        /// <returns>a <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the
        /// latest (rolling) aggregate for each key. If the aggregate function returns {@code null}, it is then interpreted as
        /// deletion for the key, and future messages of the same key coming from upstream operators
        /// will be handled as newly initialized value.</returns>
        IKTable<K, VR> Aggregate<VR, VRS>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator) where VRS : ISerDes<VR>, new();

        /// <summary>
        /// Aggregate the values of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// Aggregating is a generalization of Reducer combining via reduce(...)} as it, for example,
        /// allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IKeyValueStore{K, VR}"/> (which is basically an ever-updating materialized view)
        /// that can be queried by the given store name in {@code materialized}.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, VR}" /> changelog stream.
        /// <p>
        /// The specified <see cref="Func{VR}"/> initializer is applied once directly before the first input record is processed to
        /// provide an initial intermediate aggregation result that is used to process the first record.
        /// The specified <see cref="Func{K, V, VR, VR}"/> aggregator is applied for each input record and computes a new aggregate using the current
        /// aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// initialize) and the record's value.
        /// Thus, <see cref="Aggregate{VR}(Func{VR}, Func{K, V, VR, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> can be used to compute aggregate functions like
        /// count (c.f. <see cref="Count(Materialized{K, long, IKeyValueStore{Bytes, byte[]}}, string)"/>).
        /// </p>
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an <see cref="Func{VR}"/> initializer that computes an initial intermediate aggregation result</param>
        /// <param name="aggregator">an <see cref="Func{K, V, VR, VR}"/> aggregator that computes a new aggregate result</param>
        /// <param name="materialized">an instance of Materialized used to materialize a state store.</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the
        /// latest (rolling) aggregate for each key. If the aggregate function returns {@code null}, it is then interpreted as
        /// deletion for the key, and future messages of the same key coming from upstream operators
        /// will be handled as newly initialized value.</returns>
        IKTable<K, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator,
            Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Aggregate the values of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// Aggregating is a generalization of Reducer combining via reduce(...)} as it, for example,
        /// allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IKeyValueStore{K, VR}"/> (which is basically an ever-updating materialized view)
        /// that can be queried by the given store name in {@code materialized}.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, VR}" /> changelog stream.
        /// <p>
        /// The specified <see cref="Initializer{VR}"/> is applied once directly before the first input record is processed to
        /// provide an initial intermediate aggregation result that is used to process the first record.
        /// The specified <see cref="Aggregator{K, V, VR}"/> is applied for each input record and computes a new aggregate using the current
        /// aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// <see cref="Initializer{VR}"/>) and the record's value.
        /// Thus, <see cref="Aggregate{VR}(Initializer{VR}, Aggregator{K, V, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> can be used to compute aggregate functions like
        /// count (c.f. <see cref="Count(Materialized{K, long, IKeyValueStore{Bytes, byte[]}}, string)"/>).
        /// </p>
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an <see cref="Initializer{VR}"/>  that computes an initial intermediate aggregation result</param>
        /// <param name="aggregator">an <see cref="Aggregator{K, V, VR}"/> that computes a new aggregate result</param>
        /// <param name="materialized">an instance of Materialized used to materialize a state store.</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the
        /// latest (rolling) aggregate for each key. If the aggregate function returns {@code null}, it is then interpreted as
        /// deletion for the key, and future messages of the same key coming from upstream operators
        /// will be handled as newly initialized value.</returns>
        IKTable<K, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Combine the value of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// Combining implies that the type of the aggregate result is the same as the type of the input value
        /// The result is written into a local <see cref="IKeyValueStore{K, V}" /> (which is basically an ever-updating materialized view)
        /// provided by the given store name in materialized.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}" /> changelog stream.
        /// <p>
        /// The specified <see cref="Reducer{V}"/> is applied for each input record and computes a new aggregate using the current
        /// aggregate (first argument) and the record's value (second argument):
        /// <example>
        /// <code>
        /// (long v1, long v2) => return v1 + v2;
        /// </code>
        /// </example>
        /// </p>
        /// If there is no current aggregate the <see cref="Reducer{V}"/> is not applied and the new aggregate will be the record's
        /// value as-is.
        /// Thus, <see cref="Reduce(Reducer{V})" /> can be used to compute aggregate functions like sum, min, or
        /// max.
        /// </summary>
        /// <param name="reducer">a <see cref="Reducer{V}"/> that computes a new aggregate result. Cannot be null.</param>
        /// <returns> @return a <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys, and values that represent the
        /// latest (rolling) aggregate for each key. If the reduce function returns null, it is then interpreted as
        /// deletion for the key, and future messages of the same key coming from upstream operators
        /// will be handled as newly initialized value.</returns>
        IKTable<K, V> Reduce(Reducer<V> reducer);

        /// <summary>
        /// Combine the value of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// Combining implies that the type of the aggregate result is the same as the type of the input value
        /// The result is written into a local <see cref="IKeyValueStore{K, V}" /> (which is basically an ever-updating materialized view)
        /// provided by the given store name in materialized.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}" /> changelog stream.
        /// <p>
        /// The specified reducer function <see cref="Func{V, V, V}"/> is applied for each input record and computes a new aggregate using the current
        /// aggregate (first argument) and the record's value (second argument):
        /// <example>
        /// <code>
        /// (long v1, long v2) => return v1 + v2;
        /// </code>
        /// </example>
        /// </p>
        /// If there is no current aggregate the <see cref="Reducer{V}"/> is not applied and the new aggregate will be the record's
        /// value as-is.
        /// Thus, <see cref="Reduce(Func{V, V, V})" /> can be used to compute aggregate functions like sum, min, or
        /// max.
        /// </summary>
        /// <param name="reducer">a <see cref="Func{V, V, V}"/> reducer  that computes a new aggregate result. Cannot be null.</param>
        /// <returns> @return a <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys, and values that represent the
        /// latest (rolling) aggregate for each key. If the reduce function returns null, it is then interpreted as
        /// deletion for the key, and future messages of the same key coming from upstream operators
        /// will be handled as newly initialized value.</returns>
        IKTable<K, V> Reduce(Func<V, V, V> reducer);

        /// <summary>
        /// Combine the value of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// Combining implies that the type of the aggregate result is the same as the type of the input value
        /// The result is written into a local <see cref="IKeyValueStore{K, V}" /> (which is basically an ever-updating materialized view)
        /// provided by the given store name in materialized.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}" /> changelog stream.
        /// <p>
        /// The specified <see cref="Reducer{V}"/> is applied for each input record and computes a new aggregate using the current
        /// aggregate (first argument) and the record's value (second argument):
        /// <example>
        /// <code>
        /// (long v1, long v2) => return v1 + v2;
        /// </code>
        /// </example>
        /// </p>
        /// If there is no current aggregate the <see cref="Reducer{V}"/> is not applied and the new aggregate will be the record's
        /// value as-is.
        /// Thus, <see cref="Reduce(Reducer{V}, Materialized{K, V, IKeyValueStore{Bytes, byte[]}}, string)" /> can be used to compute aggregate functions like sum, min, or
        /// max.
        /// </summary>
        /// <param name="reducer">a <see cref="Reducer{V}"/> that computes a new aggregate result. Cannot be null.</param>
        /// <param name="materialized">an instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <param name="named">a name config used to name the processor in the topology.</param>
        /// <returns> @return a <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys, and values that represent the
        /// latest (rolling) aggregate for each key. If the reduce function returns null, it is then interpreted as
        /// deletion for the key, and future messages of the same key coming from upstream operators
        /// will be handled as newly initialized value.</returns>
        IKTable<K, V> Reduce(Reducer<V> reducer, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Combine the value of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// Combining implies that the type of the aggregate result is the same as the type of the input value
        /// The result is written into a local <see cref="IKeyValueStore{K, V}" /> (which is basically an ever-updating materialized view)
        /// provided by the given store name in materialized.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}" /> changelog stream.
        /// <p>
        /// The specified reducer function <see cref="Func{V, V, V}"/> is applied for each input record and computes a new aggregate using the current
        /// aggregate (first argument) and the record's value (second argument):
        /// <example>
        /// <code>
        /// (long v1, long v2) => return v1 + v2;
        /// </code>
        /// </example>
        /// </p>
        /// If there is no current aggregate the <see cref="Reducer{V}"/> is not applied and the new aggregate will be the record's
        /// value as-is.
        /// Thus, <see cref="Reduce(Func{V, V, V}, Materialized{K, V, IKeyValueStore{Bytes, byte[]}}, string)" /> can be used to compute aggregate functions like sum, min, or
        /// max.
        /// </summary>
        /// <param name="reducer">a <see cref="Func{V, V, V}"/> reducer  that computes a new aggregate result. Cannot be null.</param>
        /// <param name="materialized">an instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <param name="named">a name config used to name the processor in the topology.</param>
        /// <returns> @return a <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys, and values that represent the
        /// latest (rolling) aggregate for each key. If the reduce function returns null, it is then interpreted as
        /// deletion for the key, and future messages of the same key coming from upstream operators
        /// will be handled as newly initialized value.</returns>
        IKTable<K, V> Reduce(Func<V, V, V> reducer, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);


        /// <summary>
        /// Create a new <see cref="ITimeWindowedKStream{K, V}"/> instance that can be used to perform windowed aggregations.
        /// </summary>
        /// <typeparam name="W">the window type</typeparam>
        /// <param name="options">the specification of the windows aggregation</param>
        /// <returns>an instance of <see cref="ITimeWindowedKStream{K, V}"/></returns>
        ITimeWindowedKStream<K, V> WindowedBy<W>(WindowOptions<W> options)
            where W : Window;
    }
}
