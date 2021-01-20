using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;
using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// <see cref="ITimeWindowedKStream{K, V}"/> is an abstraction of a <i>windowed</i> record stream of keyvalue pairs.
    /// It is an intermediate representation after a grouping and windowing of a <see cref="IKStream{K, V}"/> before an aggregation is
    /// applied to the new (partitioned) windows resulting in a windowed <see cref="IKTable{K, V}"/> (a <emph>windowed</emph>
    /// <see cref="IKTable{K, V}"/> is a <see cref="IKTable{K, V}"/> with key type <see cref="Windowed{K}"/>).
    /// The specified windows define either hopping time windows that can be overlapping or tumbling (c.f.
    /// <see cref="HoppingWindowOptions"/> and <see cref="TumblingWindowOptions"/>)
    /// <p>
    /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating
    /// materialized view) that can be queried using the name provided in the <see cref="Materialized{K, V, S}"/> instance.
    /// Furthermore, updates to the store are sent downstream into a windowed <see cref="IKTable{K, V}"/> changelog stream, where
    /// "windowed" implies that the <see cref="IKTable{K, V}"/> key is a combined key of the original record key and a window ID.
    /// New events are added to <see cref="TimeWindowOptions"/> until their grace period ends (see <see cref="TimeWindowOptions.GracePeriodMs"/>)
    /// </p>
    /// A <see cref="ITimeWindowedKStream{K, V}"/> must be obtained from a <see cref="IKGroupedStream{K, V}"/> via
    /// <see cref="IKGroupedStream{K, V}.WindowedBy{W}(WindowOptions{W})"/>
    /// </summary>
    /// <typeparam name="K">Type of keys</typeparam>
    /// <typeparam name="V">Type of values</typeparam>
    public interface ITimeWindowedKStream<K, V>
    {
        /// <summary>
        /// Count the number of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// <p>
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the name provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// To query the local <see cref="IWindowStore{K, V}"/> it must be obtained via <see cref="KafkaStream.Store{T, K, V}(StoreQueryParameters{T, K, V})"/>.
        /// </p>
        /// <exemple>
        /// <code>
        /// KafkaStreams streams = ... // counting words
        /// Store queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
        /// var localWindowStore = streams.Store(StoreQueryParameters.FromNameAndType(queryableStoreName, QueryableStoreTypes.IWindowStore&lt;string, long&gt;()));
        /// String key = "some-word";
        /// DateTime fromTime = ...;
        /// DateTime toTime = ...;
        /// var countForWordsForWindows = localWindowStore.Fetch(key, fromTime, toTime); // key must be local (application state is shared over all running Kafka Streams instances)
        /// </code>
        /// </exemple>
        /// </summary>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys and long  that represent the latest (rolling) count (i.e., number of records) for each key within a window</returns>
        IKTable<Windowed<K>, long> Count();

        /// <summary>
        /// Count the number of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// <p>
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the name provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// To query the local <see cref="IWindowStore{K, V}"/> it must be obtained via <see cref="KafkaStream.Store{T, K, V}(StoreQueryParameters{T, K, V})"/>.
        /// </p>
        /// <exemple>
        /// <code>
        /// KafkaStreams streams = ... // counting words
        /// Store queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
        /// var localWindowStore = streams.Store(StoreQueryParameters.FromNameAndType(queryableStoreName, QueryableStoreTypes.IWindowStore&lt;string, long&gt;()));
        /// String key = "some-word";
        /// DateTime fromTime = ...;
        /// DateTime toTime = ...;
        /// var countForWordsForWindows = localWindowStore.Fetch(key, fromTime, toTime); // key must be local (application state is shared over all running Kafka Streams instances)
        /// </code>
        /// </exemple>
        /// </summary>
        /// <param name="named">a named config used to name the processor in the topology.</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys and long  that represent the latest (rolling) count (i.e., number of records) for each key within a window</returns>
        IKTable<Windowed<K>, long> Count(string named);

        /// <summary>
        /// Count the number of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// <p>
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the name provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// To query the local <see cref="IWindowStore{K, V}"/> it must be obtained via <see cref="KafkaStream.Store{T, K, V}(StoreQueryParameters{T, K, V})"/>.
        /// </p>
        /// <exemple>
        /// <code>
        /// KafkaStreams streams = ... // counting words
        /// Store queryableStoreName = ... // the queryableStoreName should be the name of the store as defined by the Materialized instance
        /// var localWindowStore = streams.Store(StoreQueryParameters.FromNameAndType(queryableStoreName, QueryableStoreTypes.IWindowStore&lt;string, long&gt;()));
        /// String key = "some-word";
        /// DateTime fromTime = ...;
        /// DateTime toTime = ...;
        /// var countForWordsForWindows = localWindowStore.Fetch(key, fromTime, toTime); // key must be local (application state is shared over all running Kafka Streams instances)
        /// </code>
        /// </exemple>
        /// </summary>
        /// <param name="materialized">an instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.(Note: the valueSerde will be automatically set to <see cref="Int64SerDes"/> if there is no valueSerde provided)</param>
        /// <param name="named">a named config used to name the processor in the topology.</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys and long  that represent the latest (rolling) count (i.e., number of records) for each key within a window</returns>
        IKTable<Windowed<K>, long> Count(Materialized<K, long, IWindowStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Aggregate the values of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// Aggregating is a generalization of <see cref="Reduce(Func{V, V, V})"/> combining via Reduce(...) as it, for example,
        /// allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the store name as provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// <p>
        /// The specified <see cref="Func{VR}"/> initializer is applied directly before the first input record (per key) in each window is
        /// processed to provide an initial intermediate aggregation result that is used to process the first record for
        /// the window (per key).
        /// The specified <see cref="Func{K, V, VR, VR}"/> aggregator is applied for each input record and computes a new aggregate using the current
        /// aggregate (or for the very first record using the intermediate aggregation result provided via the initializer
        /// <see cref="Func{VR}"/>) and the record's value.
        /// Thus, Aggregate(..) can be used to compute aggregate functions like count (c.f. <see cref="Count()"/>)
        /// </p>
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an initializer function that computes an initial intermediate aggregation result. Cannot be null.</param>
        /// <param name="aggregator">an aggregator function that computes a new aggregate result. Cannot be null.</param>
        /// <returns>a windowed <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key within a window </returns>
        IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator);

        /// <summary>
        /// Aggregate the values of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// Aggregating is a generalization of <see cref="Reduce(Func{V, V, V})"/> combining via Reduce(...) as it, for example,
        /// allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the store name as provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// <p>
        /// The specified <see cref="Initializer{VR}"/> is applied directly before the first input record (per key) in each window is
        /// processed to provide an initial intermediate aggregation result that is used to process the first record for
        /// the window (per key).
        /// The specified <see cref="Aggregator{K, V, VR}"/> is applied for each input record and computes a new aggregate using the current
        /// aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// <see cref="Initializer{VR}"/>) and the record's value.
        /// Thus, Aggregate(..) can be used to compute aggregate functions like count (c.f. <see cref="Count()"/>)
        /// </p>
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an <see cref="Initializer{VR}"/> that computes an initial intermediate aggregation result. Cannot be null.</param>
        /// <param name="aggregator">an <see cref="Aggregator{K, V, VR}"/> that computes a new aggregate result. Cannot be null.</param>
        /// <returns>a windowed <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key within a window </returns>
        IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator);

        /// <summary>
        /// Aggregate the values of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// Aggregating is a generalization of <see cref="Reduce(Func{V, V, V})"/> combining via Reduce(...) as it, for example,
        /// allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the store name as provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// <p>
        /// The specified <see cref="Func{VR}"/> initializer is applied directly before the first input record (per key) in each window is
        /// processed to provide an initial intermediate aggregation result that is used to process the first record for
        /// the window (per key).
        /// The specified <see cref="Func{K, V, VR, VR}"/> aggregator is applied for each input record and computes a new aggregate using the current
        /// aggregate (or for the very first record using the intermediate aggregation result provided via the initializer
        /// <see cref="Func{VR}"/>) and the record's value.
        /// Thus, Aggregate(..) can be used to compute aggregate functions like count (c.f. <see cref="Count()"/>)
        /// </p>
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, VR}"/></typeparam>
        /// <typeparam name="VRS">the serdes of value type</typeparam>
        /// <param name="initializer">an initializer function that computes an initial intermediate aggregation result. Cannot be null.</param>
        /// <param name="aggregator">an aggregator function that computes a new aggregate result. Cannot be null.</param>
        /// <returns>a windowed <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key within a window </returns>
        IKTable<Windowed<K>, VR> Aggregate<VR, VRS>(Func<VR> initializer, Func<K, V, VR, VR> aggregator) where VRS : ISerDes<VR>, new();

        /// <summary>
        /// Aggregate the values of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// Aggregating is a generalization of <see cref="Reduce(Func{V, V, V})"/> combining via Reduce(...) as it, for example,
        /// allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the store name as provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// <p>
        /// The specified <see cref="Initializer{VR}"/> is applied directly before the first input record (per key) in each window is
        /// processed to provide an initial intermediate aggregation result that is used to process the first record for
        /// the window (per key).
        /// The specified <see cref="Aggregator{K, V, VR}"/> is applied for each input record and computes a new aggregate using the current
        /// aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// <see cref="Initializer{VR}"/>) and the record's value.
        /// Thus, Aggregate(..) can be used to compute aggregate functions like count (c.f. <see cref="Count()"/>)
        /// </p>
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, VR}"/></typeparam>
        /// <typeparam name="VRS">the serdes of value type</typeparam>
        /// <param name="initializer">an <see cref="Initializer{VR}"/> that computes an initial intermediate aggregation result. Cannot be null.</param>
        /// <param name="aggregator">an <see cref="Aggregator{K, V, VR}"/> that computes a new aggregate result. Cannot be null.</param>
        /// <returns>a windowed <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key within a window </returns>
        IKTable<Windowed<K>, VR> Aggregate<VR, VRS>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator) where VRS : ISerDes<VR>, new();

        /// <summary>
        /// Aggregate the values of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// Aggregating is a generalization of <see cref="Reduce(Func{V, V, V})"/> combining via Reduce(...) as it, for example,
        /// allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the store name as provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// <p>
        /// The specified <see cref="Func{VR}"/> initializer is applied directly before the first input record (per key) in each window is
        /// processed to provide an initial intermediate aggregation result that is used to process the first record for
        /// the window (per key).
        /// The specified <see cref="Func{K, V, VR, VR}"/> aggregator is applied for each input record and computes a new aggregate using the current
        /// aggregate (or for the very first record using the intermediate aggregation result provided via the initializer
        /// <see cref="Func{VR}"/>) and the record's value.
        /// Thus, Aggregate(..) can be used to compute aggregate functions like count (c.f. <see cref="Count()"/>)
        /// </p>
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an initializer function that computes an initial intermediate aggregation result. Cannot be null.</param>
        /// <param name="aggregator">an aggregator function that computes a new aggregate result. Cannot be null.</param>
        /// <param name="materialized">a <see cref="Materialized{K, V, S}"/> config used to materialize a state store. Cannot be null.</param>
        /// <param name="named">a named config used to name the processor in the topology.</param>
        /// <returns>a windowed <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key within a window </returns>
        IKTable<Windowed<K>, VR> Aggregate<VR>(Func<VR> initializer, Func<K, V, VR, VR> aggregator, Materialized<K, VR, IWindowStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Aggregate the values of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// Aggregating is a generalization of <see cref="Reduce(Func{V, V, V})"/> combining via Reduce(...) as it, for example,
        /// allows the result to have a different type than the input values.
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the store name as provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// <p>
        /// The specified <see cref="Initializer{VR}"/> is applied directly before the first input record (per key) in each window is
        /// processed to provide an initial intermediate aggregation result that is used to process the first record for
        /// the window (per key).
        /// The specified <see cref="Aggregator{K, V, VR}"/> is applied for each input record and computes a new aggregate using the current
        /// aggregate (or for the very first record using the intermediate aggregation result provided via the
        /// <see cref="Initializer{VR}"/>) and the record's value.
        /// Thus, Aggregate(..) can be used to compute aggregate functions like count (c.f. <see cref="Count()"/>)
        /// </p>
        /// </summary>
        /// <typeparam name="VR">the value type of the resulting <see cref="IKTable{K, VR}"/></typeparam>
        /// <param name="initializer">an <see cref="Initializer{VR}"/> that computes an initial intermediate aggregation result. Cannot be null.</param>
        /// <param name="aggregator">an <see cref="Aggregator{K, V, VR}"/> that computes a new aggregate result. Cannot be null.</param>
        /// <param name="materialized">a <see cref="Materialized{K, V, S}"/> config used to materialize a state store. Cannot be null.</param>
        /// <param name="named">a named config used to name the processor in the topology.</param>
        /// <returns>a windowed <see cref="IKTable{K, VR}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key within a window </returns>
        IKTable<Windowed<K>, VR> Aggregate<VR>(Initializer<VR> initializer, Aggregator<K, V, VR> aggregator, Materialized<K, VR, IWindowStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Combine the values of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// Combining implies that the type of the aggregate result is the same as the type of the input value.
        /// <p>
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the store name as provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// </p>
        /// <para>
        /// The value of the first record per window initialized the aggregation result.
        /// The specified <see cref="Reducer{V}"/> is applied for each additional input record per window and computes a new
        /// aggregate using the current aggregate (first argument) and the record's value (second argument):
        /// Thus, reducer can be used to compute aggregate functions like sum, min, or max.
        /// </para>
        /// </summary>
        /// <param name="reducer">a <see cref="Reducer{V}"/> that computes a new aggregate result. Cannot be null.</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key within a window. </returns>
        IKTable<Windowed<K>, V> Reduce(Reducer<V> reducer);

        /// <summary>
        /// Combine the values of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// Combining implies that the type of the aggregate result is the same as the type of the input value.
        /// <p>
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the store name as provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// </p>
        /// <para>
        /// The value of the first record per window initialized the aggregation result.
        /// The specified <see cref="Func{V, V, V}"/> reducer is applied for each additional input record per window and computes a new
        /// aggregate using the current aggregate (first argument) and the record's value (second argument):
        /// Thus, reducer can be used to compute aggregate functions like sum, min, or max.
        /// </para>
        /// </summary>
        /// <param name="reducer">a <see cref="Func{V, V, V}"/> reducer that computes a new aggregate result. Cannot be null.</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key within a window. </returns>
        IKTable<Windowed<K>, V> Reduce(Func<V, V, V> reducer);

        /// <summary>
        /// Combine the values of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// Combining implies that the type of the aggregate result is the same as the type of the input value.
        /// <p>
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the store name as provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// </p>
        /// <para>
        /// The value of the first record per window initialized the aggregation result.
        /// The specified <see cref="Reducer{V}"/> is applied for each additional input record per window and computes a new
        /// aggregate using the current aggregate (first argument) and the record's value (second argument):
        /// Thus, reducer can be used to compute aggregate functions like sum, min, or max.
        /// </para>
        /// </summary>
        /// <param name="reducer">a <see cref="Reducer{V}"/> that computes a new aggregate result. Cannot be null.</param>
        /// <param name="materialized">a <see cref="Materialized{K, V, S}"/> config used to materialize a state store.</param>
        /// <param name="named">a named config used to name the processor in the topology.</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key within a window. </returns>
        IKTable<Windowed<K>, V> Reduce(Reducer<V> reducer, Materialized<K, V, IWindowStore<Bytes, byte[]>> materialized, string named = null);

        /// <summary>
        /// Combine the values of records in this stream by the grouped key and defined windows.
        /// Records with null key or value are ignored.
        /// Combining implies that the type of the aggregate result is the same as the type of the input value.
        /// <p>
        /// The result is written into a local <see cref="IWindowStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// that can be queried using the store name as provided with <see cref="Materialized{K, V, S}"/>.
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}"/> changelog stream.
        /// </p>
        /// <para>
        /// The value of the first record per window initialized the aggregation result.
        /// The specified <see cref="Func{V, V, V}"/> reducer is applied for each additional input record per window and computes a new
        /// aggregate using the current aggregate (first argument) and the record's value (second argument):
        /// Thus, reducer can be used to compute aggregate functions like sum, min, or max.
        /// </para>
        /// </summary>
        /// <param name="reducer">a <see cref="Func{V, V, V}"/> reducer that computes a new aggregate result. Cannot be null.</param>
        /// <param name="materialized">a <see cref="Materialized{K, V, S}"/> config used to materialize a state store.</param>
        /// <param name="named">a named config used to name the processor in the topology.</param>
        /// <returns>a windowed <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys, and values that represent the latest (rolling) aggregate for each key within a window. </returns>
        IKTable<Windowed<K>, V> Reduce(Func<V, V, V> reducer, Materialized<K, V, IWindowStore<Bytes, byte[]>> materialized, string named = null);
    }
}
