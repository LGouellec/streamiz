using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using System;

namespace Streamiz.Kafka.Net.Table
{
    /// <summary>
    /// <see cref="IKGroupedTable{K, V}"/> is an abstraction of a re-grouped changelog stream from a primary-keyed table,
    /// usually on a different grouping key than the original primary key.
    /// A <see cref="IKGroupedTable{K, V}"/> must be obtained from a <see cref="IKTable{K, V}"/> via <see cref="IKTable{K, V}.GroupBy{KR, VR}(System.Func{K, V, System.Collections.Generic.KeyValuePair{KR, VR}}, string)"/>
    /// </summary>
    /// <typeparam name="K">Type of key</typeparam>
    /// <typeparam name="V">Type of value</typeparam>
    public interface IKGroupedTable<K, V>
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
        /// Combine the value of records of the original <see cref="IKTable{K, V}"/> that got <see cref="IKTable{K, V}.GroupBy{KR, VR}(Func{K, V, System.Collections.Generic.KeyValuePair{KR, VR}}, string)"/>
        /// to the same key into a new instance of <see cref="IKTable{K, V}"/>.
        /// Records with null key are ignored.
        /// Combining implies that the type of the aggregate result is the same as the type of the input value.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the provided store name.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKeyValueStore{K, V}"/>  changelog stream.
        /// <p>
        /// Each update to the original <see cref="IKTable{K, V}"/> results in a two step update of the result <see cref="IKTable{K, V}"/>.
        /// The specified <see cref="Reducer{V}"/> adder is applied for each update record and computes a new aggregate using the
        /// current aggregate (first argument) and the record's value (second argument) by adding the new record to the
        /// aggregate.
        /// The specified <see cref="Reducer{V}"/> subtractor is applied for each "replaced" record of the original <see cref="IKTable{K, V}"/>
        /// and computes a new aggregate using the current aggregate (first argument) and the record's value (second
        /// argument) by "removing" the "replaced" record from the aggregate.
        /// If there is no current aggregate the <see cref="Reducer{V}"/> is not applied and the new aggregate will be the record's
        /// value as-is.
        /// </p>
        /// Thus, <see cref="Reduce(Reducer{V}, Reducer{V})" /> can be used to compute aggregate functions like sum.
        /// <example>
        /// <code>
        /// public class SumAdder : Reducer&lt;int&gt; {
        ///   public int Apply(int currentAgg, int newValue) {
        ///     return currentAgg + newValue;
        ///   }
        /// }
        /// public class SumSubtractor : Reducer&lt;int&gt; {
        ///   public int Apply(int currentAgg, int oldValue) {
        ///     return currentAgg - oldValue;
        ///   }
        /// }
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="adder">a <see cref="Reducer{V}"/> that adds a new value to the aggregate result</param>
        /// <param name="substractor">a <see cref="Reducer{V}"/> that removed an old value from the aggregate result</param>
        /// <returns>a <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key</returns>
        IKTable<K, V> Reduce(Reducer<V> adder, Reducer<V> substractor);

        /// <summary>
        /// Combine the value of records of the original <see cref="IKTable{K, V}"/> that got <see cref="IKTable{K, V}.GroupBy{KR, VR}(Func{K, V, System.Collections.Generic.KeyValuePair{KR, VR}}, string)"/>
        /// to the same key into a new instance of <see cref="IKTable{K, V}"/>.
        /// Records with null key are ignored.
        /// Combining implies that the type of the aggregate result is the same as the type of the input value.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the provided store name.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKeyValueStore{K, V}"/>  changelog stream.
        /// <p>
        /// Each update to the original <see cref="IKTable{K, V}"/> results in a two step update of the result <see cref="IKTable{K, V}"/>.
        /// The specified <see cref="Func{V, V, V}"/> adder is applied for each update record and computes a new aggregate using the
        /// current aggregate (first argument) and the record's value (second argument) by adding the new record to the
        /// aggregate.
        /// The specified <see cref="Func{V, V, V}"/> subtractor is applied for each "replaced" record of the original <see cref="IKTable{K, V}"/>
        /// and computes a new aggregate using the current aggregate (first argument) and the record's value (second
        /// argument) by "removing" the "replaced" record from the aggregate.
        /// If there is no current aggregate the <see cref="Func{V, V, V}"/> is not applied and the new aggregate will be the record's
        /// value as-is.
        /// </p>
        /// Thus, <see cref="Reduce(Func{V, V, V}, Func{V, V, V})" /> can be used to compute aggregate functions like sum.
        /// </summary>
        /// <param name="adder">a <see cref="Func{V, V, V}"/> that adds a new value to the aggregate result</param>
        /// <param name="substractor">a <see cref="Func{V, V, V}"/> that removed an old value from the aggregate result</param>
        /// <returns>a <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key</returns>
        IKTable<K, V> Reduce(Func<V, V, V> adder, Func<V, V, V> substractor);

        /// <summary>
        /// Combine the value of records of the original <see cref="IKTable{K, V}"/> that got <see cref="IKTable{K, V}.GroupBy{KR, VR}(Func{K, V, System.Collections.Generic.KeyValuePair{KR, VR}}, string)"/>
        /// to the same key into a new instance of <see cref="IKTable{K, V}"/>.
        /// Records with null key are ignored.
        /// Combining implies that the type of the aggregate result is the same as the type of the input value.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the provided store name.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKeyValueStore{K, V}"/>  changelog stream.
        /// <p>
        /// Each update to the original <see cref="IKTable{K, V}"/> results in a two step update of the result <see cref="IKTable{K, V}"/>.
        /// The specified <see cref="Reducer{V}"/> adder is applied for each update record and computes a new aggregate using the
        /// current aggregate (first argument) and the record's value (second argument) by adding the new record to the
        /// aggregate.
        /// The specified <see cref="Reducer{V}"/> subtractor is applied for each "replaced" record of the original <see cref="IKTable{K, V}"/>
        /// and computes a new aggregate using the current aggregate (first argument) and the record's value (second
        /// argument) by "removing" the "replaced" record from the aggregate.
        /// If there is no current aggregate the <see cref="Reducer{V}"/> is not applied and the new aggregate will be the record's
        /// value as-is.
        /// </p>
        /// Thus, <see cref="Reduce(Reducer{V}, Reducer{V})" /> can be used to compute aggregate functions like sum.
        /// <example>
        /// <code>
        /// public class SumAdder : Reducer&lt;int&gt; {
        ///   public int Apply(int currentAgg, int newValue) {
        ///     return currentAgg + newValue;
        ///   }
        /// }
        /// public class SumSubtractor : Reducer&lt;int&gt; {
        ///   public int Apply(int currentAgg, int oldValue) {
        ///     return currentAgg - oldValue;
        ///   }
        /// }
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="adder">a <see cref="Reducer{V}"/> that adds a new value to the aggregate result</param>
        /// <param name="substractor">a <see cref="Reducer{V}"/> that removed an old value from the aggregate result</param>
        /// <param name="materialized">the instance of materialized used to materialize the state store</param>
        /// <param name="named">a named config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key</returns>
        IKTable<K, V> Reduce(Reducer<V> adder, Reducer<V> substractor, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Combine the value of records of the original <see cref="IKTable{K, V}"/> that got <see cref="IKTable{K, V}.GroupBy{KR, VR}(Func{K, V, System.Collections.Generic.KeyValuePair{KR, VR}}, string)"/>
        /// to the same key into a new instance of <see cref="IKTable{K, V}"/>.
        /// Records with null key are ignored.
        /// Combining implies that the type of the aggregate result is the same as the type of the input value.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the provided store name.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKeyValueStore{K, V}"/>  changelog stream.
        /// <p>
        /// Each update to the original <see cref="IKTable{K, V}"/> results in a two step update of the result <see cref="IKTable{K, V}"/>.
        /// The specified <see cref="Func{V, V, V}"/> adder is applied for each update record and computes a new aggregate using the
        /// current aggregate (first argument) and the record's value (second argument) by adding the new record to the
        /// aggregate.
        /// The specified <see cref="Func{V, V, V}"/> subtractor is applied for each "replaced" record of the original <see cref="IKTable{K, V}"/>
        /// and computes a new aggregate using the current aggregate (first argument) and the record's value (second
        /// argument) by "removing" the "replaced" record from the aggregate.
        /// If there is no current aggregate the <see cref="Func{V, V, V}"/> is not applied and the new aggregate will be the record's
        /// value as-is.
        /// </p>
        /// Thus, <see cref="Reduce(Func{V, V, V}, Func{V, V, V})" /> can be used to compute aggregate functions like sum.
        /// </summary>
        /// <param name="adder">a <see cref="Func{V, V, V}"/> that adds a new value to the aggregate result</param>
        /// <param name="substractor">a <see cref="Func{V, V, V}"/> that removed an old value from the aggregate result</param>
        /// <param name="materialized">the instance of materialized used to materialize the state store</param>
        /// <param name="named">a named config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key</returns>
        IKTable<K, V> Reduce(Func<V, V, V> adder, Func<V, V, V> substractor, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Aggregate the value of records of the original <see cref="IKTable{K, V}"/> that got <see cref="IKTable{K, V}.GroupBy{KR, VR}(Func{K, V, System.Collections.Generic.KeyValuePair{KR, VR}}, string)"/>
        /// to the same key into a new instance of <see cref="IKTable{K, V}"/>.
        /// Records with null key are ignored.
        /// Aggregating is a generalization of <see cref="Reduce(Func{V, V, V}, Func{V, V, V}, Materialized{K, V, IKeyValueStore{Bytes, byte[]}}, string)"/> combining via Reduce(...) as it,
        /// for example, allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the provided store name.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// <p>
        /// The specified <see cref="Func{VR}"/> initializer is applied once directly before the first input record is processed to
        /// provide an initial intermediate aggregation result that is used to process the first record.
        /// Each update to the original <see cref="IKTable{K, V}"/> results in a two step update of the result <see cref="IKTable{K, V}"/> .
        /// The specified <see cref="Func{K, V, VR, VR}"/> adder is applied for each update record and computes a new aggregate using the
        /// current aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// <see cref="Func{VR}"/> initializer) and the record's value by adding the new record to the aggregate.
        /// The specified <see cref="Func{K, V, VR, VR}"/> subtractor is applied for each "replaced" record of the original <see cref="IKTable{K, V}"/>
        /// and computes a new aggregate using the current aggregate and the record's value by "removing" the "replaced"
        /// record from the aggregate.
        /// Thus, <see cref="Aggregate{VR}(Func{VR}, Func{K, V, VR, VR}, Func{K, V, VR, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> can be used to compute aggregate functions
        /// like sum.
        /// </p>
        /// For sum, the initializer, adder, and subtractor would work as follows:
        /// </summary>
        /// <typeparam name="VR">the value type of the aggregated <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an <see cref="Initializer{VR}"/> that provides an initial aggregate result value</param>
        /// <param name="adder">an <see cref="Aggregator{K, V, VR}"/> that removed an old record from the aggregate result</param>
        /// <param name="subtractor">an <see cref="Aggregator{K, V, VR}"/> that removed an old record from the aggregate result</param>
        /// <returns> a <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key</returns>
        IKTable<K, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> adder, Func<K, V, VR, VR> subtractor);

        /// <summary>
        /// Aggregate the value of records of the original <see cref="IKTable{K, V}"/> that got <see cref="IKTable{K, V}.GroupBy{KR, VR}(Func{K, V, System.Collections.Generic.KeyValuePair{KR, VR}}, string)"/>
        /// to the same key into a new instance of <see cref="IKTable{K, V}"/>.
        /// Records with null key are ignored.
        /// Aggregating is a generalization of <see cref="Reduce(Func{V, V, V}, Func{V, V, V}, Materialized{K, V, IKeyValueStore{Bytes, byte[]}}, string)"/> combining via Reduce(...) as it,
        /// for example, allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the provided store name.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// <p>
        /// The specified <see cref="Initializer{VR}"/> is applied once directly before the first input record is processed to
        /// provide an initial intermediate aggregation result that is used to process the first record.
        /// Each update to the original <see cref="IKTable{K, V}"/> results in a two step update of the result <see cref="IKTable{K, V}"/> .
        /// The specified <see cref="Aggregator{K, V, VR}"/> adder is applied for each update record and computes a new aggregate using the
        /// current aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// <see cref="Initializer{VR}"/>) and the record's value by adding the new record to the aggregate.
        /// The specified <see cref="Aggregator{K, V, VR}"/> subtractor is applied for each "replaced" record of the original <see cref="IKTable{K, V}"/>
        /// and computes a new aggregate using the current aggregate and the record's value by "removing" the "replaced"
        /// record from the aggregate.
        /// Thus, <see cref="Aggregate{VR}(Func{VR}, Func{K, V, VR, VR}, Func{K, V, VR, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> can be used to compute aggregate functions
        /// like sum.
        /// </p>
        /// For sum, the initializer, adder, and subtractor would work as follows:
        /// <example>
        /// <code>
        /// public class SumInitializer : Initializer&lt;long&gt; {
        ///   public long Apply() => 0L;
        /// }
        /// 
        /// public class SumAdder : Aggregator&lt;string, string, long&gt; {
        ///   public long Apply(string key, string newValue, long aggregate) {
        ///     return aggregate + newValue.Length;
        ///   }
        /// }
        /// 
        /// public class SumSubtractor : Aggregator&lt;string, string, long&gt; {
        ///   public long Apply(string key, string oldValue, long aggregate) {
        ///     return aggregate - oldValue.Length;
        ///   }
        /// }
        /// </code>
        /// </example>
        /// </summary>
        /// <typeparam name="VR">the value type of the aggregated <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an <see cref="Initializer{VR}"/> that provides an initial aggregate result value</param>
        /// <param name="adder">an <see cref="Aggregator{K, V, VR}"/> that removed an old record from the aggregate result</param>
        /// <param name="subtractor">an <see cref="Aggregator{K, V, VR}"/> that removed an old record from the aggregate result</param>
        /// <returns> a <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key</returns>
        IKTable<K, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> adder, Aggregator<K, V, VR> subtractor);

        /// <summary>
        /// Aggregate the value of records of the original <see cref="IKTable{K, V}"/> that got <see cref="IKTable{K, V}.GroupBy{KR, VR}(Func{K, V, System.Collections.Generic.KeyValuePair{KR, VR}}, string)"/>
        /// to the same key into a new instance of <see cref="IKTable{K, V}"/>.
        /// Records with null key are ignored.
        /// Aggregating is a generalization of <see cref="Reduce(Func{V, V, V}, Func{V, V, V}, Materialized{K, V, IKeyValueStore{Bytes, byte[]}}, string)"/> combining via Reduce(...) as it,
        /// for example, allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the provided store name.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// <p>
        /// The specified <see cref="Func{VR}"/> initializer is applied once directly before the first input record is processed to
        /// provide an initial intermediate aggregation result that is used to process the first record.
        /// Each update to the original <see cref="IKTable{K, V}"/> results in a two step update of the result <see cref="IKTable{K, V}"/> .
        /// The specified <see cref="Func{K, V, VR, VR}"/> adder is applied for each update record and computes a new aggregate using the
        /// current aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// <see cref="Func{VR}"/> initializer) and the record's value by adding the new record to the aggregate.
        /// The specified <see cref="Func{K, V, VR, VR}"/> subtractor is applied for each "replaced" record of the original <see cref="IKTable{K, V}"/>
        /// and computes a new aggregate using the current aggregate and the record's value by "removing" the "replaced"
        /// record from the aggregate.
        /// Thus, <see cref="Aggregate{VR}(Func{VR}, Func{K, V, VR, VR}, Func{K, V, VR, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> can be used to compute aggregate functions
        /// like sum.
        /// </p>
        /// For sum, the initializer, adder, and subtractor would work as follows:
        /// </summary>
        /// <typeparam name="VR">the value type of the aggregated <see cref="IKTable{K, VR}"/></typeparam>
        /// <typeparam name="VRS">the value serdes type of the aggregated <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an <see cref="Initializer{VR}"/> that provides an initial aggregate result value</param>
        /// <param name="adder">an <see cref="Aggregator{K, V, VR}"/> that removed an old record from the aggregate result</param>
        /// <param name="subtractor">an <see cref="Aggregator{K, V, VR}"/> that removed an old record from the aggregate result</param>
        /// <returns> a <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key</returns>
        IKTable<K, VR> Aggregate<VR, VRS>(Func<VR> initializer, Func<K, V, VR, VR> adder, Func<K, V, VR, VR> subtractor) where VRS : ISerDes<VR>, new();

        /// <summary>
        /// Aggregate the value of records of the original <see cref="IKTable{K, V}"/> that got <see cref="IKTable{K, V}.GroupBy{KR, VR}(Func{K, V, System.Collections.Generic.KeyValuePair{KR, VR}}, string)"/>
        /// to the same key into a new instance of <see cref="IKTable{K, V}"/>.
        /// Records with null key are ignored.
        /// Aggregating is a generalization of <see cref="Reduce(Func{V, V, V}, Func{V, V, V}, Materialized{K, V, IKeyValueStore{Bytes, byte[]}}, string)"/> combining via Reduce(...) as it,
        /// for example, allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the provided store name.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// <p>
        /// The specified <see cref="Initializer{VR}"/> is applied once directly before the first input record is processed to
        /// provide an initial intermediate aggregation result that is used to process the first record.
        /// Each update to the original <see cref="IKTable{K, V}"/> results in a two step update of the result <see cref="IKTable{K, V}"/> .
        /// The specified <see cref="Aggregator{K, V, VR}"/> adder is applied for each update record and computes a new aggregate using the
        /// current aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// <see cref="Initializer{VR}"/>) and the record's value by adding the new record to the aggregate.
        /// The specified <see cref="Aggregator{K, V, VR}"/> subtractor is applied for each "replaced" record of the original <see cref="IKTable{K, V}"/>
        /// and computes a new aggregate using the current aggregate and the record's value by "removing" the "replaced"
        /// record from the aggregate.
        /// Thus, <see cref="Aggregate{VR}(Func{VR}, Func{K, V, VR, VR}, Func{K, V, VR, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> can be used to compute aggregate functions
        /// like sum.
        /// </p>
        /// For sum, the initializer, adder, and subtractor would work as follows:
        /// <example>
        /// <code>
        /// public class SumInitializer : Initializer&lt;long&gt; {
        ///   public long Apply() => 0L;
        /// }
        /// 
        /// public class SumAdder : Aggregator&lt;string, string, long&gt; {
        ///   public long Apply(string key, string newValue, long aggregate) {
        ///     return aggregate + newValue.Length;
        ///   }
        /// }
        /// 
        /// public class SumSubtractor : Aggregator&lt;string, string, long&gt; {
        ///   public long Apply(string key, string oldValue, long aggregate) {
        ///     return aggregate - oldValue.Length;
        ///   }
        /// }
        /// </code>
        /// </example>
        /// </summary>
        /// <typeparam name="VR">the value type of the aggregated <see cref="IKTable{K, VR}"/></typeparam>
        /// <typeparam name="VRS">the value serdes type of the aggregated <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an <see cref="Initializer{VR}"/> that provides an initial aggregate result value</param>
        /// <param name="adder">an <see cref="Aggregator{K, V, VR}"/> that removed an old record from the aggregate result</param>
        /// <param name="subtractor">an <see cref="Aggregator{K, V, VR}"/> that removed an old record from the aggregate result</param>
        /// <returns> a <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key</returns>
        IKTable<K, VR> Aggregate<VR, VRS>(Initializer<VR> initializer, Aggregator<K, V, VR> adder, Aggregator<K, V, VR> subtractor) where VRS : ISerDes<VR>, new();

        /// <summary>
        /// Aggregate the value of records of the original <see cref="IKTable{K, V}"/> that got <see cref="IKTable{K, V}.GroupBy{KR, VR}(Func{K, V, System.Collections.Generic.KeyValuePair{KR, VR}}, string)"/>
        /// to the same key into a new instance of <see cref="IKTable{K, V}"/>.
        /// Records with null key are ignored.
        /// Aggregating is a generalization of <see cref="Reduce(Func{V, V, V}, Func{V, V, V}, Materialized{K, V, IKeyValueStore{Bytes, byte[]}}, string)"/> combining via Reduce(...) as it,
        /// for example, allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the provided store name.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// <p>
        /// The specified <see cref="Func{VR}"/> initializer is applied once directly before the first input record is processed to
        /// provide an initial intermediate aggregation result that is used to process the first record.
        /// Each update to the original <see cref="IKTable{K, V}"/> results in a two step update of the result <see cref="IKTable{K, V}"/> .
        /// The specified <see cref="Func{K, V, VR, VR}"/> adder is applied for each update record and computes a new aggregate using the
        /// current aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// <see cref="Func{VR}"/> initializer) and the record's value by adding the new record to the aggregate.
        /// The specified <see cref="Func{K, V, VR, VR}"/> subtractor is applied for each "replaced" record of the original <see cref="IKTable{K, V}"/>
        /// and computes a new aggregate using the current aggregate and the record's value by "removing" the "replaced"
        /// record from the aggregate.
        /// Thus, <see cref="Aggregate{VR}(Func{VR}, Func{K, V, VR, VR}, Func{K, V, VR, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> can be used to compute aggregate functions
        /// like sum.
        /// </p>
        /// For sum, the initializer, adder, and subtractor would work as follows:
        /// </summary>
        /// <typeparam name="VR">the value type of the aggregated <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an <see cref="Initializer{VR}"/> that provides an initial aggregate result value</param>
        /// <param name="adder">an <see cref="Aggregator{K, V, VR}"/> that removed an old record from the aggregate result</param>
        /// <param name="subtractor">an <see cref="Aggregator{K, V, VR}"/> that removed an old record from the aggregate result</param>
        /// <param name="materialized">the instance of materialized used to materialize the state store.</param>
        /// <param name="named">a named config used to name the processor in the topology></param>
        /// <returns> a <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key</returns>
        IKTable<K, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> adder, Func<K, V, VR, VR> subtractor, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Aggregate the value of records of the original <see cref="IKTable{K, V}"/> that got <see cref="IKTable{K, V}.GroupBy{KR, VR}(Func{K, V, System.Collections.Generic.KeyValuePair{KR, VR}}, string)"/>
        /// to the same key into a new instance of <see cref="IKTable{K, V}"/>.
        /// Records with null key are ignored.
        /// Aggregating is a generalization of <see cref="Reduce(Func{V, V, V}, Func{V, V, V}, Materialized{K, V, IKeyValueStore{Bytes, byte[]}}, string)"/> combining via Reduce(...) as it,
        /// for example, allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the provided store name.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// <p>
        /// The specified <see cref="Initializer{VR}"/> is applied once directly before the first input record is processed to
        /// provide an initial intermediate aggregation result that is used to process the first record.
        /// Each update to the original <see cref="IKTable{K, V}"/> results in a two step update of the result <see cref="IKTable{K, V}"/> .
        /// The specified <see cref="Aggregator{K, V, VR}"/> adder is applied for each update record and computes a new aggregate using the
        /// current aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// <see cref="Initializer{VR}"/>) and the record's value by adding the new record to the aggregate.
        /// The specified <see cref="Aggregator{K, V, VR}"/> subtractor is applied for each "replaced" record of the original <see cref="IKTable{K, V}"/>
        /// and computes a new aggregate using the current aggregate and the record's value by "removing" the "replaced"
        /// record from the aggregate.
        /// Thus, <see cref="Aggregate{VR}(Func{VR}, Func{K, V, VR, VR}, Func{K, V, VR, VR}, Materialized{K, VR, IKeyValueStore{Bytes, byte[]}}, string)"/> can be used to compute aggregate functions
        /// like sum.
        /// </p>
        /// For sum, the initializer, adder, and subtractor would work as follows:
        /// <example>
        /// <code>
        /// public class SumInitializer : Initializer&lt;long&gt; {
        ///   public long Apply() => 0L;
        /// }
        /// 
        /// public class SumAdder : Aggregator&lt;string, string, long&gt; {
        ///   public long Apply(string key, string newValue, long aggregate) {
        ///     return aggregate + newValue.Length;
        ///   }
        /// }
        /// 
        /// public class SumSubtractor : Aggregator&lt;string, string, long&gt; {
        ///   public long Apply(string key, string oldValue, long aggregate) {
        ///     return aggregate - oldValue.Length;
        ///   }
        /// }
        /// </code>
        /// </example>
        /// </summary>
        /// <typeparam name="VR">the value type of the aggregated <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an <see cref="Initializer{VR}"/> that provides an initial aggregate result value</param>
        /// <param name="adder">an <see cref="Aggregator{K, V, VR}"/> that removed an old record from the aggregate result</param>
        /// <param name="subtractor">an <see cref="Aggregator{K, V, VR}"/> that removed an old record from the aggregate result</param>
        /// <param name="materialized">the instance of materialized used to materialize the state store.</param>
        /// <param name="named">a named config used to name the processor in the topology></param>
        /// <returns> a <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key</returns>
        IKTable<K, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> adder, Aggregator<K, V, VR> subtractor, Materialized<K, VR, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);
    }
}