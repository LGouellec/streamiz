using Kafka.Streams.Net.Processors;
using Kafka.Streams.Net.SerDes;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Kafka.Streams.Net.Stream
{
    /// <summary>
    /// <see cref="IKStream{K, V}"/> is an abstraction of a record stream of <see cref="KeyValuePair{K, V}"/> pairs, i.e., each record is an independent entity/event in the real world.
    /// For example a user X might buy two items I1 and I2, and thus there might be two records &lt;K:I1&gt;, &lt;K:I2&gt; in the stream.
    /// A <see cref="IKStream{K, V}"/> is either <see cref="StreamBuilder.Stream(string, StreamOptions)"/> defined from one topic that
    /// are consumed message by message or the result of a <see cref="IKStream{K, V}"/> transformation.
    /// A <see cref="Table.IKTable{K, V}"/> can also be <see cref="Table.IKTable{K, V}.ToStream(string)"/> converted into a <see cref="IKStream{K, V}"/>.
    /// </summary>
    /// <typeparam name="K">Type of key</typeparam>
    /// <typeparam name="V">Type of value</typeparam>
    public interface IKStream<K, V>
    {
        /// <summary>
        /// Creates an array of <see cref="IKStream{K, V}"/> from this stream by branching the records in the original stream based on
        /// the supplied predicates.
        /// Each record is evaluated against the supplied predicates, and predicates are evaluated in order.
        /// 
        /// Each stream in the result array corresponds position-wise (index) to the predicate in the supplied predicates.
        /// 
        /// The branching happens on first-match: A record in the original stream is assigned to the corresponding result
        /// stream for the first predicate that evaluates to true, and is assigned to this stream only.
        /// A record will be dropped if none of the predicates evaluate to true.
        /// This is a stateless record-by-record operation.
        /// </summary>
        /// <param name="predicates">the ordered list of predicate instances</param>
        /// <returns>Multiple distinct substreams of this <see cref="IKStream{K, V}"/></returns>
        IKStream<K, V>[] Branch(params Func<K, V, bool>[] predicates);

        /// <summary>
        /// Creates an array of <see cref="IKStream{K, V}"/> from this stream by branching the records in the original stream based on
        /// the supplied predicates.
        /// Each record is evaluated against the supplied predicates, and predicates are evaluated in order.
        /// 
        /// Each stream in the result array corresponds position-wise (index) to the predicate in the supplied predicates.
        /// 
        /// The branching happens on first-match: A record in the original stream is assigned to the corresponding result
        /// stream for the first predicate that evaluates to true, and is assigned to this stream only.
        /// A record will be dropped if none of the predicates evaluate to true.
        /// This is a stateless record-by-record operation.
        /// </summary>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <param name="predicates">the ordered list of predicate instances</param>
        /// <returns></returns>
        IKStream<K, V>[] Branch(string named, params Func<K, V, bool>[] predicates);

        /// <summary>
        /// Create a new <see cref="IKStream{K, V}"/>
        /// that consists of all records of this stream which satisfy the given predicate.
        /// All records that DO NOT satisfy the predicate are dropped.
        /// This is a stateless record-by-record operation.
        /// </summary>
        /// <param name="predicate">A filter predicate that is applied to each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, V}"/> that contains only those records that satisfy the given predicate</returns>
        IKStream<K, V> Filter(Func<K, V, bool> predicate, string named = null);

        /// <summary>
        /// Create a new <see cref="IKStream{K, V}"/>
        /// that consists of all records of this stream which do NOT satisfy the given predicate.
        /// All records that DO satisfy the predicate are dropped.
        /// This is a stateless record-by-record operation.
        /// </summary>
        /// <param name="predicate">A filter predicate that is applied to each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, V}"/> that contains only those records that DO NOT satisfy the given predicate</returns>
        IKStream<K, V> FilterNot(Func<K, V, bool> predicate, string named = null);

        /// <summary>
        /// Materialize this stream to a topic using default serializers specified in the config and producer's.
        /// The specified topic should be manually created before it is used(i.e., before the Kafka Streams application is
        /// started).
        /// </summary>
        /// <param name="topicName">the topic name</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        void To(string topicName, string named = null);

        /// <summary>
        /// Dynamically materialize this stream to topics using default serializers specified in the config and producer's.
        /// The topic names for each record to send to is dynamically determined based on the <code>Func&lt;K, V, IRecordContext, string&gt;</code>.
        /// </summary>
        /// <param name="topicExtractor">Extractor function to determine the name of the Kafka topic to write to for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        void To(Func<K, V, IRecordContext, string> topicExtractor, string named = null);

        /// <summary>
        /// Dynamically materialize this stream to topics using default serializers specified in the config and producer's.
        /// The topic names for each record to send to is dynamically determined based on the <see cref="ITopicNameExtractor&lt;K, V&gt;"/>}.
        /// </summary>
        /// <param name="topicExtractor">The extractor to determine the name of the Kafka topic to write to for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        void To(ITopicNameExtractor<K, V> topicExtractor, string named = null);

        /// <summary>
        /// Materialize this stream to a topic using <typeparamref name="KS"/> and <typeparamref name="VS"/> serializers specified in the method parameters.
        /// The specified topic should be manually created before it is used(i.e., before the Kafka Streams application is
        /// started).
        /// </summary>
        /// <typeparam name="KS">New type key serializer</typeparam>
        /// <typeparam name="VS">New type value serializer</typeparam>
        /// <param name="topicName">the topic name</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        void To<KS, VS>(string topicName, string named = null) where KS : ISerDes<K>, new() where VS : ISerDes<V>, new();

        /// <summary>
        /// Dynamically materialize this stream to a topic using <typeparamref name="KS"/> and <typeparamref name="VS"/> serializers specified in the method parameters.
        /// The topic names for each record to send to is dynamically determined based on the <code>Func&lt;K, V, IRecordContext, string&gt;</code>.
        /// </summary>
        /// <typeparam name="KS">New type key serializer</typeparam>
        /// <typeparam name="VS">New type value serializer</typeparam>
        /// <param name="topicExtractor">Extractor function to determine the name of the Kafka topic to write to for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        void To<KS, VS>(Func<K, V, IRecordContext, string> topicExtractor, string named = null) where KS : ISerDes<K>, new() where VS : ISerDes<V>, new();

        /// <summary>
        /// Dynamically materialize this stream to a topic using <typeparamref name="KS"/> and <typeparamref name="VS"/> serializers specified in the method parameters.
        /// The topic names for each record to send to is dynamically determined based on the <see cref="ITopicNameExtractor&lt;K, V&gt;"/>}.
        /// </summary>
        /// <typeparam name="KS">New type key serializer</typeparam>
        /// <typeparam name="VS">New type value serializer</typeparam>
        /// <param name="topicExtractor">The extractor to determine the name of the Kafka topic to write to for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        void To<KS, VS>(ITopicNameExtractor<K, V> topicExtractor, string named = null) where KS : ISerDes<K>, new() where VS : ISerDes<V>, new();

        /// <summary>
        /// Transform each record of the input stream into zero or more records in the output stream (bot
        /// can be altered arbitrarily).
        /// The provided <see cref="IKeyValueMapper{K, V, VR}"/> is applied to each input record and computes zero or more
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="KR"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The provided <see cref="IKeyValueMapper{K, V, VR}"/> must return an <see cref="IEnumerable"/> and the return value must not be null.
        /// Flat-mapping records might result in an internal data redistribution if a key based operator 
        /// or join) is applied to the result <see cref="IKStream{KR, VR}"/>.
        /// </summary>
        /// <typeparam name="KR">the key type of the result stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IKeyValueMapper{K, V, VR}"/> mapper that computes the new output records</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, VR}"/> that contains more or less records with new key and value (possibly of different type)</returns>
        IKStream<KR, VR> FlatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named = null);

        /// <summary>
        /// Transform each record of the input stream into zero or more records in the output stream (bot
        /// can be altered arbitrarily).
        /// The provided <code>Func&lt;K, V, IEnumerable&lt;KeyValuePair&lt;KR,VR&gt;&gt;&gt;</code> is applied to each input record and computes zero or more
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="KR"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The provided <code>Func&lt;K, V, IEnumerable&lt;KeyValuePair&lt;KR,VR&gt;&gt;&gt;</code> must return an <see cref="IEnumerable"/> and the return value must not be null.
        /// Flat-mapping records might result in an internal data redistribution if a key based operator 
        /// or join) is applied to the result <see cref="IKStream{KR, VR}"/>.
        /// </summary>
        /// <typeparam name="KR">the key type of the result stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A function mapper that computes the new output records</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, VR}"/> that contains more or less records with new key and value (possibly of different type)</returns>
        IKStream<KR, VR> FlatMap<KR, VR>(Func<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named = null);

        /// <summary>
        /// Transform each record of the input stream into zero or more records with the same (unmodified) key in the output stream.
        /// The provided <see cref="IValueMapper{V, VR}"/> is applied to each input record and computes zero or more
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The provided <see cref="IValueMapper{V, VR}"/> must return an <see cref="IEnumerable{VR}"/> and the return value must not be null.
        /// Flat-mapping records might result in an internal data redistribution if a key based operator 
        /// or join) is applied to the result <see cref="IKStream{K, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IValueMapper{V, VR}"/> mapper that computes the new output records</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, VR}"/> that contains more or less records with new key and value (possibly of different type)</returns>
        IKStream<K, VR> FlatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper, string named = null);

        /// <summary>
        /// Transform each record of the input stream into zero or more records with the same (unmodified) key in the output stream.
        /// The provided <code>Func&lt;V, IEnumerable&lt;VR&gt;</code> is applied to each input record and computes zero or more
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The provided <code>Func&lt;V, IEnumerable&lt;VR&gt;</code> must return an <see cref="IEnumerable"/> and the return value must not be null.
        /// Flat-mapping records might result in an internal data redistribution if a key based operator 
        /// or join) is applied to the result <see cref="IKStream{KR, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A function mapper that computes the new output records</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, VR}"/> that contains more or less records with new key and value (possibly of different type)</returns>
        IKStream<K, VR> FlatMapValues<VR>(Func<V, IEnumerable<VR>> mapper, string named = null);

        /// <summary>
        /// Transform each record of the input stream into zero or more records with the same (unmodified) key in the output stream.
        /// The provided <see cref="IValueMapperWithKey{K, V, VR}"/> is applied to each input record and computes zero or more
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The provided <see cref="IValueMapperWithKey{K, V, VR}"/> must return an <see cref="IEnumerable{VR}"/> and the return value must not be null.
        /// Flat-mapping records might result in an internal data redistribution if a key based operator 
        /// or join) is applied to the result <see cref="IKStream{K, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IValueMapperWithKey{K, V, VR}"/> mapper that computes the new output records</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, VR}"/> that contains more or less records with new key and value (possibly of different type)</returns>
        IKStream<K, VR> FlatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper, string named = null);

        /// <summary>
        /// Transform each record of the input stream into zero or more records with the same (unmodified) key in the output stream.
        /// The provided <code>Func&lt;K, V, IEnumerable&lt;VR&gt;</code> is applied to each input record and computes zero or more
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The provided <code>Func&lt;K, V, IEnumerable&lt;VR&gt;</code> must return an <see cref="IEnumerable"/> and the return value must not be null.
        /// Flat-mapping records might result in an internal data redistribution if a key based operator 
        /// or join) is applied to the result <see cref="IKStream{KR, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A function mapper that computes the new output records</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, VR}"/> that contains more or less records with new key and value (possibly of different type)</returns>
        IKStream<K, VR> FlatMapValues<VR>(Func<K, V, IEnumerable<VR>> mapper, string named = null);

        /// <summary>
        /// Perform an action on each record of {@code KStream}.
        /// This is a stateless record-by-record operation
        /// Note that this is a terminal operation that returns void.
        /// </summary>
        /// <param name="action">An action to perform on each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        void Foreach(Action<K, V> action, string named = null);

        /// <summary>
        /// Print the records of this KStream using the options provided by <see cref="Printed{K, V}"/>
        /// Note that this is mainly for debugging/testing purposes, and it will try to flush on each record print.
        /// It SHOULD NOT be used for production usage if performance requirements are concerned.
        /// </summary>
        /// <param name="printed">Printed options for printing</param>          
        void Print(Printed<K, V> printed);

        /// <summary>
        /// Transform each record of the input stream into a new record in the output stream (both key and value type can be
        /// altered arbitrarily).
        /// The provided <see cref="IKeyValueMapper{K, V, VR}"/> is applied to each input record and computes a new output record.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="KR"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The example below normalizes the String key to upper-case letters and counts the number of token of the value string.
        /// Mapping records might result in an internal data redistribution if a key based operator (like an aggregation or
        /// join) is applied to the result <see cref="IKStream{KR, VR}"/>. (<seealso cref="IKStream{K, V}.MapValues{VR}(IValueMapper{V, VR}, string)"/>
        /// </summary>
        /// <typeparam name="KR">the key type of the result stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IKeyValueMapper{K, V, VR}"/> mapper that computes a new output record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, VR}"/> that contains records with new key and value (possibly both of different type)</returns>
        IKStream<KR, VR> Map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper, string named = null);

        /// <summary>
        /// Transform each record of the input stream into a new record in the output stream (both key and value type can be
        /// altered arbitrarily).
        /// The provided <code>Func&lt;K, V, KeyValuePair&lt;KR, VR&gt;&gt;</code> is applied to each input record and computes a new output record.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="KR"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The example below normalizes the String key to upper-case letters and counts the number of token of the value string.
        /// <example>
        /// <code>
        /// var stream = builder.Stream&lt;string, string&gt;("topic");
        /// var outerStream = stream.Map((k,v) => new KeyValuePair<typeparamref name="KR"/>, <typeparamref name="VR"/>>(k.ToUpperCase(), v.Split(" ").Length)
        /// </code>
        /// </example>
        /// Mapping records might result in an internal data redistribution if a key based operator (like an aggregation or
        /// join) is applied to the result <see cref="IKStream{KR, VR}"/>. (<seealso cref="IKStream{K, V}.MapValues{VR}(Func{K, V, VR}, string)"/>
        /// </summary>
        /// <typeparam name="KR">the key type of the result stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A mapper function that computes a new output record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, VR}"/> that contains records with new key and value (possibly both of different type)</returns>
        IKStream<KR, VR> Map<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> mapper, string named = null);

        /// <summary>
        /// Transform the value of each input record into a new value (with possible new type) of the output record.
        /// The provided <see cref="IValueMapper{V, VR}"/> is applied to each input record value and computes a new value for it.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;.
        /// This is a stateless record-by-record operation.
        /// Setting a new value preserves data co-location with respect to the key.
        /// Thus, NO internal data redistribution is required if a key based operator (like an aggregation or join) is applied to the result <see cref="IKStream{K, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IValueMapper{V, VR}"/> mapper that computes a new output value</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, VR}"/> that contains records with unmodified key and new values (possibly of different type)</returns>
        IKStream<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, string named = null);

        /// <summary>
        /// Transform the value of each input record into a new value (with possible new type) of the output record.
        /// The provided <code>Func&lt;V, VR&gt;</code> is applied to each input record value and computes a new value for it.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;.
        /// This is a stateless record-by-record operation.
        /// <example>
        /// <code>
        /// var stream = builder.Stream&lt;string, string&gt;("topic");
        /// var outerStream = stream.MapValues((v) => v.Split(" ").Length)
        /// </code>
        /// </example>
        /// Setting a new value preserves data co-location with respect to the key.
        /// Thus, NO internal data redistribution is required if a key based operator (like an aggregation or join) is applied to the result <see cref="IKStream{K, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A function mapper that computes a new output value</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, VR}"/> that contains records with unmodified key and new values (possibly of different type)</returns>
        IKStream<K, VR> MapValues<VR>(Func<V, VR> mapper, string named = null);

        /// <summary>
        /// Transform the value of each input record into a new value (with possible new type) of the output record.
        /// The provided <see cref="IValueMapperWithKey{K, V, VR}"/> is applied to each input record value and computes a new value for it.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;.
        /// This is a stateless record-by-record operation.
        /// Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
        /// Setting a new value preserves data co-location with respect to the key.
        /// Thus, NO internal data redistribution is required if a key based operator (like an aggregation or join) is applied to the result <see cref="IKStream{K, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IValueMapperWithKey{K, V, VR}"/> mapper that computes a new output value</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, VR}"/> that contains records with unmodified key and new values (possibly of different type)</returns>
        IKStream<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, string named = null);

        /// <summary>
        /// Transform the value of each input record into a new value (with possible new type) of the output record.
        /// The provided <code>Func&lt;K, V, VR&gt;</code> is applied to each input record value and computes a new value for it.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;.
        /// This is a stateless record-by-record operation.
        /// <example>
        /// <code>
        /// var stream = builder.Stream&lt;string, string&gt;("topic");
        /// var outerStream = stream.MapValues((k,v) => v.Split(" ").Length)
        /// </code>
        /// </example>
        /// Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
        /// Setting a new value preserves data co-location with respect to the key.
        /// Thus, NO internal data redistribution is required if a key based operator (like an aggregation or join) is applied to the result <see cref="IKStream{K, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A function mapper that computes a new output value</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, VR}"/> that contains records with unmodified key and new values (possibly of different type)</returns>
        IKStream<K, VR> MapValues<VR>(Func<K, V, VR> mapper, string named = null);

        /// <summary>
        /// Perform an action on each record of <see cref="IKStream{K, V}"/>.
        /// This is a stateless record-by-record operation.
        /// Peek is a non-terminal operation that triggers a side effect(such as logging or statistics collection)
        /// and returns an unchanged stream.
        /// Note that since this operation is stateless, it may execute multiple times for a single record in failure cases.
        /// </summary>
        /// <param name="action">An action to perform on each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>Itself</returns>
        IKStream<K, V> Peek(Action<K, V> action, string named = null);

        /// <summary>
        /// Set a new key (with possibly new type) for each input record.
        /// The provided <see cref="IKeyValueMapper{K, V, VR}"/> is applied to each input record and computes a new key for it.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="KR"/>, <typeparamref name="V"/>&gt;.
        /// This is a stateless record-by-record operation.
        /// For example, you can use this transformation to set a key for a key-less input record &lt;null,<typeparamref name="V"/>&gt; by
        /// extracting a key from the value within your <see cref="IKeyValueMapper{K, V, VR}"/>
        /// The example below computes the new key as the length of the value string.
        /// <example>
        /// <code>
        /// var stream = builder.Stream&lt;string, string&gt;("key-less-topic");
        /// var keyStream = stream.SelectKey((k,v) => v.Length);
        /// </code>
        /// </example>
        /// Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
        /// join) is applied to the result <see cref="IKStream{KR, V}"/>
        /// </summary>
        /// <typeparam name="KR">the new key type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IKeyValueMapper{K, V, VR}"/> mapper that computes a new key for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, V}"/> that contains records with new key (possibly of different type) and unmodified value</returns>
        IKStream<KR, V> SelectKey<KR>(IKeyValueMapper<K, V, KR> mapper, string named = null);

        /// <summary>
        /// Set a new key (with possibly new type) for each input record.
        /// The provided <code>Func&lt;K, V, VR&gt;</code> is applied to each input record and computes a new key for it.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="KR"/>, <typeparamref name="V"/>&gt;.
        /// This is a stateless record-by-record operation.
        /// For example, you can use this transformation to set a key for a key-less input record &lt;null,<typeparamref name="V"/>&gt; by
        /// extracting a key from the value within your <code>Func&lt;K, V, VR&gt;</code>
        /// The example below computes the new key as the length of the value string.
        /// <example>
        /// <code>
        /// var stream = builder.Stream&lt;string, string&gt;("key-less-topic");
        /// var keyStream = stream.SelectKey((k,v) => v.Length);
        /// </code>
        /// </example>
        /// Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
        /// join) is applied to the result <see cref="IKStream{KR, V}"/>
        /// </summary>
        /// <typeparam name="KR">the new key type of the result stream</typeparam>
        /// <param name="mapper">A function mapper that computes a new key for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, V}"/> that contains records with new key (possibly of different type) and unmodified value</returns>
        IKStream<KR, V> SelectKey<KR>(Func<K, V, KR> mapper, string named = null);

        /// <summary>
        /// Group the records of this <see cref="IKStream{K, V}"/> on a new key that is selected using the provided <see cref="IKeyValueMapper{K, V, VR}"/> and default serializers and deserializers.
        /// Grouping a stream on the record key is required before an aggregation operator can be applied to the data <see cref="IKGroupedStream{KR, V}"/>
        /// The provider <see cref="IKeyValueMapper{K, V, VR}"/> selects a new key (which may or may not be of the same type) while preserving the
        /// original values.
        /// If the new record key is null the record will not be included in the resulting.
        /// Because a new key is selected, an internal repartitioning topic may need to be created in Kafka if a
        /// later operator depends on the newly selected key.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>.
        /// "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// All data of this stream will be redistributed through the repartitioning topic by writing all records to it,
        /// and rereading all records from it, such that the resulting <see cref="IKGroupedStream{KR, V}"/> is partitioned on the new key.
        /// This operation is equivalent to calling <see cref="IKStream{K, V}.SelectKey{KR}(Func{K, V, KR}, string)"/> followed by <see cref="IKStream{K, V}.GroupByKey(string)"/>.
        /// If the key type is changed, it is recommended to use <see cref="IKStream{K, V}.GroupBy{KR}(Func{K, V, KR}, string)"/> instead.
        /// </summary>
        /// <typeparam name="KR">the key type of the result</typeparam>
        /// <param name="keySelector">A <see cref="IKeyValueMapper{K, V, VR}"/> selector that computes a new key for grouping</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKGroupedStream{KR, V}"/> that contains the grouped records of the original <see cref="IKStream{K, V}"/></returns>
        IKGroupedStream<KR, V> GroupBy<KR>(IKeyValueMapper<K, V, KR> keySelector, string named = null);

        /// <summary>
        /// Group the records of this <see cref="IKStream{K, V}"/> on a new key that is selected using the provided <code>Func&lt;K, V, VR&gt;</code> and default serializers and deserializers.
        /// Grouping a stream on the record key is required before an aggregation operator can be applied to the data <see cref="IKGroupedStream{KR, V}"/>
        /// The provider <code>Func&lt;K, V, VR&gt;</code> selects a new key (which may or may not be of the same type) while preserving the
        /// original values.
        /// If the new record key is null the record will not be included in the resulting.
        /// Because a new key is selected, an internal repartitioning topic may need to be created in Kafka if a
        /// later operator depends on the newly selected key.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>.
        /// "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// All data of this stream will be redistributed through the repartitioning topic by writing all records to it,
        /// and rereading all records from it, such that the resulting <see cref="IKGroupedStream{KR, V}"/> is partitioned on the new key.
        /// This operation is equivalent to calling <see cref="IKStream{K, V}.SelectKey{KR}(Func{K, V, KR}, string)"/> followed by <see cref="IKStream{K, V}.GroupByKey(string)"/>.
        /// If the key type is changed, it is recommended to use <see cref="IKStream{K, V}.GroupBy{KR}(Func{K, V, KR}, string)"/> instead.
        /// </summary>
        /// <typeparam name="KR">the key type of the result</typeparam>
        /// <param name="keySelector">A function selector that computes a new key for grouping</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKGroupedStream{KR, V}"/> that contains the grouped records of the original <see cref="IKStream{K, V}"/></returns>
        IKGroupedStream<KR, V> GroupBy<KR>(Func<K, V, KR> keySelector, string named = null);

        /// <summary>
        /// Group the records of this <see cref="IKStream{K, V}"/> on a new key that is selected using the provided <see cref="IKeyValueMapper{K, V, VR}"/> and default serializers and deserializers.
        /// Grouping a stream on the record key is required before an aggregation operator can be applied to the data <see cref="IKGroupedStream{KR, V}"/>
        /// The provider <see cref="IKeyValueMapper{K, V, VR}"/> selects a new key (which may or may not be of the same type) while preserving the
        /// original values.
        /// If the new record key is null the record will not be included in the resulting.
        /// Because a new key is selected, an internal repartitioning topic may need to be created in Kafka if a
        /// later operator depends on the newly selected key.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>.
        /// "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// All data of this stream will be redistributed through the repartitioning topic by writing all records to it,
        /// and rereading all records from it, such that the resulting <see cref="IKGroupedStream{KR, V}"/> is partitioned on the new key.
        /// This operation is equivalent to calling <see cref="IKStream{K, V}.SelectKey{KR}(Func{K, V, KR}, string)"/> followed by <see cref="IKStream{K, V}.GroupByKey(string)"/>.
        /// If the key type is changed, it is recommended to use <see cref="IKStream{K, V}.GroupBy{KR}(Func{K, V, KR}, string)"/> instead.
        /// </summary>
        /// <typeparam name="KR">the key type of the result</typeparam>
        /// <typeparam name="KRS">New serializer for <typeparamref name="KR"/> type</typeparam>
        /// <param name="keySelector">A <see cref="IKeyValueMapper{K, V, VR}"/> selector that computes a new key for grouping</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKGroupedStream{KR, V}"/> that contains the grouped records of the original <see cref="IKStream{K, V}"/></returns>
        IKGroupedStream<KR, V> GroupBy<KR, KRS>(IKeyValueMapper<K, V, KR> keySelector, string named = null) where KRS : ISerDes<KR>, new();

        /// <summary>
        /// Group the records of this <see cref="IKStream{K, V}"/> on a new key that is selected using the provided <code>Func&lt;K, V, VR&gt;</code> and default serializers and deserializers.
        /// Grouping a stream on the record key is required before an aggregation operator can be applied to the data <see cref="IKGroupedStream{KR, V}"/>
        /// The provider <code>Func&lt;K, V, VR&gt;</code> selects a new key (which may or may not be of the same type) while preserving the
        /// original values.
        /// If the new record key is null the record will not be included in the resulting.
        /// Because a new key is selected, an internal repartitioning topic may need to be created in Kafka if a
        /// later operator depends on the newly selected key.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>.
        /// "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// All data of this stream will be redistributed through the repartitioning topic by writing all records to it,
        /// and rereading all records from it, such that the resulting <see cref="IKGroupedStream{KR, V}"/> is partitioned on the new key.
        /// This operation is equivalent to calling <see cref="IKStream{K, V}.SelectKey{KR}(Func{K, V, KR}, string)"/> followed by <see cref="IKStream{K, V}.GroupByKey(string)"/>.
        /// If the key type is changed, it is recommended to use <see cref="IKStream{K, V}.GroupBy{KR}(Func{K, V, KR}, string)"/> instead.
        /// </summary>
        /// <typeparam name="KR">the key type of the result</typeparam>
        /// <typeparam name="KRS">New serializer for <typeparamref name="KR"/> type</typeparam>
        /// <param name="keySelector">A function selector that computes a new key for grouping</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKGroupedStream{KR, V}"/> that contains the grouped records of the original <see cref="IKStream{K, V}"/></returns>
        IKGroupedStream<KR, V> GroupBy<KR, KRS>(Func<K, V, KR> keySelector, string named = null) where KRS : ISerDes<KR>, new();

        /// <summary>
        /// Group the records by their current key into a <see cref="IKGroupedStream{K, V}"/> while preserving the original values
        /// and default serializers and deserializers.
        /// Grouping a stream on the record key is required before an aggregation operator can be applied to the data.
        /// If a record key is null , the record will not be included in the resulting.
        /// If a key changing operator was used before this operation and no data redistribution happened afterwards an internal repartitioning topic may need to be created in Kafka if a later
        /// operator depends on the newly selected key.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>,
        /// "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// For this case, all data of this stream will be redistributed through the repartitioning topic by writing all
        /// records to it, and rereading all records from it, such that the resulting <see cref="IKGroupedStream{K, V}"/> is partitioned
        /// correctly on its key.
        /// If the last key changing operator changed the key type, it is recommended to use <see cref="IKStream{K, V}.GroupByKey{KS, VS}(string)"/>.
        /// </summary>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKGroupedStream{K, V}"/> that contains the grouped records of the original <see cref="IKStream{K, V}"/></returns>
        IKGroupedStream<K, V> GroupByKey(string named = null);

        /// <summary>
        /// Group the records by their current key into a <see cref="IKGroupedStream{K, V}"/> while preserving the original values
        /// and default serializers and deserializers.
        /// Grouping a stream on the record key is required before an aggregation operator can be applied to the data.
        /// If a record key is null , the record will not be included in the resulting.
        /// If a key changing operator was used before this operation and no data redistribution happened afterwards an internal repartitioning topic may need to be created in Kafka if a later
        /// operator depends on the newly selected key.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>,
        /// "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// For this case, all data of this stream will be redistributed through the repartitioning topic by writing all
        /// records to it, and rereading all records from it, such that the resulting <see cref="IKGroupedStream{K, V}"/> is partitioned
        /// correctly on its key.
        /// If the last key changing operator changed the key type, it is recommended to use <see cref="IKStream{K, V}.GroupByKey(string)"/>.
        /// </summary>
        /// <typeparam name="KS">Serializer for <typeparamref name="K"/></typeparam>
        /// <typeparam name="VS">Serializer for <typeparamref name="V"/></typeparam>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKGroupedStream{K, V}"/> that contains the grouped records of the original <see cref="IKStream{K, V}"/></returns>
        IKGroupedStream<K, V> GroupByKey<KS, VS>(string named = null) where KS : ISerDes<K>, new() where VS : ISerDes<V>, new();
    }
}
