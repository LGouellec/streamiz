using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Table;
using System;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// <see cref="StreamBuilder"/> provide the high-level Kafka Streams DSL to specify a Kafka Streams topology.
    /// </summary>
    public class StreamBuilder
    {
        private readonly Topology topology = new Topology();
        private readonly InternalTopologyBuilder internalTopologyBuilder;
        private readonly InternalStreamBuilder internalStreamBuilder;

        /// <summary>
        /// Constructor without arguments
        /// </summary>
        public StreamBuilder()
        {
            internalTopologyBuilder = topology.Builder;
            internalStreamBuilder = new InternalStreamBuilder(internalTopologyBuilder);
        }

        #region KStream

        #region KStream<K, V>

        /// <summary>
        /// Create a <see cref="KStream{K, V}"/> from the specified topic.
        /// The default "auto.offset.reset" strategy, default <see cref="ITimestampExtractor"/> and default key and value
        /// deserializers as specified in the <see cref="IStreamConfig"/> are used.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case it is the user's responsibility to repartition the data before any key based operation
        /// (like aggregation or join) is applied to the returned <see cref="IKStream{K, V}"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <returns>A <see cref="IKStream{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKStream<K, V> Stream<K, V>(string topic)
            => Stream<K, V>(topic, null, null);

        /// <summary>
        /// Create a <see cref="KStream{K, V}"/> from the specified topic.
        /// The default "auto.offset.reset" strategy and default <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case it is the user's responsibility to repartition the data before any key based operation
        /// (like aggregation or join) is applied to the returned <see cref="IKStream{K, V}"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <returns>A <see cref="IKStream{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKStream<K, V> Stream<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            => Stream(topic, keySerdes, valueSerdes, null, null);

        /// <summary>
        /// Create a <see cref="KStream{K, V}"/> from the specified topic.
        /// The default "auto.offset.reset" strategy and default <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case it is the user's responsibility to repartition the data before any key based operation
        /// (like aggregation or join) is applied to the returned <see cref="IKStream{K, V}"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <param name="named">Processor name</param>
        /// <returns>A <see cref="IKStream{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKStream<K, V> Stream<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, string named)
            => Stream(topic, keySerdes, valueSerdes, named, null);

        /// <summary>
        /// Create a <see cref="KStream{K, V}"/> from the specified topic.
        /// The default "auto.offset.reset" strategy as specified in the <see cref="IStreamConfig"/> are used.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case it is the user's responsibility to repartition the data before any key based operation
        /// (like aggregation or join) is applied to the returned <see cref="IKStream{K, V}"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <param name="extractor">the timestamp extractor to used. If null the default timestamp extractor from config will be used</param>
        /// <returns>A <see cref="IKStream{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKStream<K, V> Stream<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, ITimestampExtractor extractor)
            => Stream(topic, keySerdes, valueSerdes, null, extractor);

        /// <summary>
        /// Create a <see cref="KStream{K, V}"/> from the specified topic.
        /// The default "auto.offset.reset" strategy and default <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case it is the user's responsibility to repartition the data before any key based operation
        /// (like aggregation or join) is applied to the returned <see cref="IKStream{K, V}"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <param name="named">Processor name</param>
        /// <param name="extractor">the timestamp extractor to used. If null the default timestamp extractor from config will be used</param>
        /// <returns>A <see cref="IKStream{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKStream<K, V> Stream<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, string named, ITimestampExtractor extractor)
        {
            if (string.IsNullOrEmpty(topic))
            {
                throw new ArgumentException("Topic of KStream must not be null or empty");
            }

            var consumedInternal = new ConsumedInternal<K, V>(named, keySerdes, valueSerdes, extractor);
            return internalStreamBuilder.Stream(topic, consumedInternal);
        }

        #endregion

        #region KStream<K, V, KS, VS>

        /// <summary>
        /// Create a <see cref="KStream{K, V}"/> from the specified topic.
        /// The default "auto.offset.reset" strategy, default <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case it is the user's responsibility to repartition the data before any key based operation
        /// (like aggregation or join) is applied to the returned <see cref="IKStream{K, V}"/>.
        /// </summary>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <returns>A <see cref="IKStream{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKStream<K, V> Stream<K, V, KS, VS>(string topic)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Stream<K, V, KS, VS>(topic, null, null);

        /// <summary>
        /// Create a <see cref="KStream{K, V}"/> from the specified topic.
        /// The default "auto.offset.reset" strategy, default <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case it is the user's responsibility to repartition the data before any key based operation
        /// (like aggregation or join) is applied to the returned <see cref="IKStream{K, V}"/>.
        /// </summary>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="named">Processor name</param>
        /// <returns>A <see cref="IKStream{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKStream<K, V> Stream<K, V, KS, VS>(string topic, string named)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Stream<K, V, KS, VS>(topic, named, null);

        /// <summary>
        /// Create a <see cref="KStream{K, V}"/> from the specified topic.
        /// The default "auto.offset.reset" strategy, default <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case it is the user's responsibility to repartition the data before any key based operation
        /// (like aggregation or join) is applied to the returned <see cref="IKStream{K, V}"/>.
        /// </summary>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="extractor">the timestamp extractor to used. If null the default timestamp extractor from config will be used</param>
        /// <returns>A <see cref="IKStream{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKStream<K, V> Stream<K, V, KS, VS>(string topic, ITimestampExtractor extractor)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Stream<K, V, KS, VS>(topic, null, extractor);

        /// <summary>
        /// Create a <see cref="KStream{K, V}"/> from the specified topic.
        /// The default "auto.offset.reset" strategy, default <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case it is the user's responsibility to repartition the data before any key based operation
        /// (like aggregation or join) is applied to the returned <see cref="IKStream{K, V}"/>.
        /// </summary>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="named">Processor name</param>
        /// <param name="extractor">the timestamp extractor to used. If null the default timestamp extractor from config will be used</param>
        /// <returns>A <see cref="IKStream{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKStream<K, V> Stream<K, V, KS, VS>(string topic, string named, ITimestampExtractor extractor)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Stream(topic, new KS(), new VS(), named, extractor);

        #endregion

        #endregion

        #region KTable

        #region KTable<K, V>

        /// <summary>
        /// Create a <see cref="IKTable{K, V}"/> for the specified topic.
        /// The default "auto.offset.reset" strategy, <see cref="ITimestampExtractor"/>, key and value deserializers
        /// as specified in the <see cref="IStreamConfig"/> are used.
        /// Input keyvalue records with null key will be dropped.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case the returned <see cref="IKTable{K, V}"/> will be corrupted.
        /// 
        /// The resulting <see cref="IKTable{K, V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <returns>a <see cref="IKTable{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKTable<K, V> Table<K, V>(string topic)
            => Table<K, V>(topic, null, null);

        /// <summary>
        /// Create a <see cref="IKTable{K, V}"/> for the specified topic.
        /// The default "auto.offset.reset" strategy, <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// Input keyvalue records with null key will be dropped.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case the returned <see cref="IKTable{K, V}"/> will be corrupted.
        /// 
        /// The resulting <see cref="IKTable{K, V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <returns>a <see cref="IKTable{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKTable<K, V> Table<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            => Table<K, V>(topic, keySerdes, valueSerdes, null);

        /// <summary>
        /// Create a <see cref="IKTable{K, V}"/> for the specified topic.
        /// The default "auto.offset.reset" strategy, <see cref="ITimestampExtractor"/>, key and value deserializers
        /// as specified in the <see cref="IStreamConfig"/> are used.
        /// Input keyvalue records with null key will be dropped.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case the returned <see cref="IKTable{K, V}"/> will be corrupted.
        /// 
        /// The resulting <see cref="IKTable{K, V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <returns>a <see cref="IKTable{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKTable<K, V> Table<K, V>(string topic, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            => Table<K, V>(topic, null, null, materialized);

        /// <summary>
        /// Create a <see cref="IKTable{K, V}"/> for the specified topic.
        /// The default "auto.offset.reset" strategy, <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// Input keyvalue records with null key will be dropped.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case the returned <see cref="IKTable{K, V}"/> will be corrupted.
        /// 
        /// The resulting <see cref="IKTable{K, V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <returns>a <see cref="IKTable{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKTable<K, V> Table<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            => Table<K, V>(topic, keySerdes, valueSerdes, materialized, null, null);

        /// <summary>
        /// Create a <see cref="IKTable{K, V}"/> for the specified topic.
        /// The default "auto.offset.reset" strategy, <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// Input keyvalue records with null key will be dropped.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case the returned <see cref="IKTable{K, V}"/> will be corrupted.
        /// 
        /// The resulting <see cref="IKTable{K, V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <param name="named">Processor name</param>
        /// <returns>a <see cref="IKTable{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKTable<K, V> Table<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named)
            => Table<K, V>(topic, keySerdes, valueSerdes, materialized, named, null);

        /// <summary>
        /// Create a <see cref="IKTable{K, V}"/> for the specified topic.
        /// Input keyvalue records with null key will be dropped.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case the returned <see cref="IKTable{K, V}"/> will be corrupted.
        /// 
        /// The resulting <see cref="IKTable{K, V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <param name="named">Processor name</param>
        /// <param name="extractor">the timestamp extractor to be used. If null the default timestamp extractor from config will be used</param>
        /// <returns>a <see cref="IKTable{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKTable<K, V> Table<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named, ITimestampExtractor extractor)
        {
            if (string.IsNullOrEmpty(topic))
            {
                throw new ArgumentException("Topic of KTable must not be null or empty");
            }

            materialized = materialized ?? Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.Create();

            var consumedInternal = new ConsumedInternal<K, V>(named, keySerdes, valueSerdes, extractor);
            materialized.UseProvider(internalStreamBuilder, $"{topic}-").InitConsumed(consumedInternal);

            return internalStreamBuilder.Table(topic, consumedInternal, materialized);
        }

        #endregion

        #region KTable<K, V, KS, VS>

        /// <summary>
        /// Create a <see cref="IKTable{K, V}"/> for the specified topic.
        /// The default "auto.offset.reset" strategy, <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// Input keyvalue records with null key will be dropped.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case the returned <see cref="IKTable{K, V}"/> will be corrupted.
        /// 
        /// The resulting <see cref="IKTable{K, V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <returns>a <see cref="IKTable{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKTable<K, V> Table<K, V, KS, VS>(string topic)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Table<K, V, KS, VS>(topic, null, null, null);

        /// <summary>
        /// Create a <see cref="IKTable{K, V}"/> for the specified topic.
        /// The default "auto.offset.reset" strategy, <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// Input keyvalue records with null key will be dropped.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case the returned <see cref="IKTable{K, V}"/> will be corrupted.
        /// 
        /// The resulting <see cref="IKTable{K, V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <returns>a <see cref="IKTable{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKTable<K, V> Table<K, V, KS, VS>(string topic, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Table<K, V, KS, VS>(topic, materialized, null, null);

        /// <summary>
        /// Create a <see cref="IKTable{K, V}"/> for the specified topic.
        /// The default "auto.offset.reset" strategy, <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// Input keyvalue records with null key will be dropped.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case the returned <see cref="IKTable{K, V}"/> will be corrupted.
        /// 
        /// The resulting <see cref="IKTable{K, V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <param name="named">Processor name</param>
        /// <returns>a <see cref="IKTable{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKTable<K, V> Table<K, V, KS, VS>(string topic, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Table<K, V, KS, VS>(topic, materialized, named, null);

        /// <summary>
        /// Create a <see cref="IKTable{K, V}"/> for the specified topic.
        /// The default "auto.offset.reset" strategy, <see cref="ITimestampExtractor"/> as specified in the <see cref="IStreamConfig"/> are used.
        /// Input keyvalue records with null key will be dropped.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case the returned <see cref="IKTable{K, V}"/> will be corrupted.
        /// 
        /// The resulting <see cref="IKTable{K, V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <param name="extractor">the timestamp extractor to used. If null the default timestamp extractor from config will be used</param>
        /// <returns>a <see cref="IKTable{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKTable<K, V> Table<K, V, KS, VS>(string topic, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, ITimestampExtractor extractor)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Table<K, V, KS, VS>(topic, materialized, null, extractor);

        /// <summary>
        /// Create a <see cref="IKTable{K, V}"/> for the specified topic.
        /// Input keyvalue records with null key will be dropped.
        /// 
        /// Note that the specified input topic must be partitioned by key.
        /// If this is not the case the returned <see cref="IKTable{K, V}"/> will be corrupted.
        /// 
        /// The resulting <see cref="IKTable{K, V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <param name="named">Processor name</param>
        /// <param name="extractor">the timestamp extractor to be used. If null the default timestamp extractor from config will be used</param>
        /// <returns>a <see cref="IKTable{K, V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IKTable<K, V> Table<K, V, KS, VS>(string topic, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named, ITimestampExtractor extractor)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Table<K, V>(topic, new KS(), new VS(), materialized, named, extractor);

        #endregion

        #endregion

        #region GlobalKTable

        #region GlobalKTable<K, V>

        /// <summary>
        /// Create a <see cref="IGlobalKTable{K,V}"/> for the specified topic.
        /// The default <see cref="ITimestampExtractor"/>, key and value deserializers
        /// as specified in the <see cref="IStreamConfig"/> are used.
        /// Input keyvalue records with <code>null</code> key will be dropped.
        /// The resulting <see cref="IGlobalKTable{K,V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/>
        /// with an internal store name. Note that store name may not be queriable through Interactive Queries.
        /// No internal changelog topic is created since the original input topic can be used for recovery.
        /// Note that <see cref="IGlobalKTable{K,V}"/> always applies <code>"auto.offset.reset"</code> strategy <code>"earliest"</code>
        /// regardless of the specified value in <see cref="IStreamConfig"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <returns>a <see cref="IGlobalKTable{K,V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IGlobalKTable<K, V> GlobalTable<K, V>(string topic) => GlobalTable<K, V>(topic, null, null);

        /// <summary>
        /// Create a <see cref="IGlobalKTable{K,V}"/> for the specified topic.
        /// The default <see cref="ITimestampExtractor"/>, as specified in the <see cref="IStreamConfig"/> is used.
        /// Input keyvalue records with <code>null</code> key will be dropped.
        /// The resulting <see cref="IGlobalKTable{K,V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/>
        /// with an internal store name. Note that store name may not be queriable through Interactive Queries.
        /// No internal changelog topic is created since the original input topic can be used for recovery.
        /// Note that <see cref="IGlobalKTable{K,V}"/> always applies <code>"auto.offset.reset"</code> strategy <code>"earliest"</code>
        /// regardless of the specified value in <see cref="IStreamConfig"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <returns>a <see cref="IGlobalKTable{K,V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IGlobalKTable<K, V> GlobalTable<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            => GlobalTable(topic, keySerdes, valueSerdes, null);

        /// <summary>
        /// Create a <see cref="IGlobalKTable{K,V}"/> for the specified topic.
        /// The default <see cref="ITimestampExtractor"/>, key and value deserializers
        /// as specified in the <see cref="IStreamConfig"/> are used.
        /// Input keyvalue records with <code>null</code> key will be dropped.
        /// The resulting <see cref="IGlobalKTable{K,V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// However, no internal changelog topic is created since the original input topic can be used for recovery.
        /// Note that <see cref="IGlobalKTable{K,V}"/> always applies <code>"auto.offset.reset"</code> strategy <code>"earliest"</code>
        /// regardless of the specified value in <see cref="IStreamConfig"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <returns>a <see cref="IGlobalKTable{K,V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IGlobalKTable<K, V> GlobalTable<K, V>(string topic, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            => GlobalTable(topic, null, null, materialized);

        /// <summary>
        /// Create a <see cref="IGlobalKTable{K,V}"/> for the specified topic.
        /// The default <see cref="ITimestampExtractor"/>, as specified in the <see cref="IStreamConfig"/> is used.
        /// Input keyvalue records with <code>null</code> key will be dropped.
        /// The resulting <see cref="IGlobalKTable{K,V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// However, no internal changelog topic is created since the original input topic can be used for recovery.
        /// Note that <see cref="IGlobalKTable{K,V}"/> always applies <code>"auto.offset.reset"</code> strategy <code>"earliest"</code>
        /// regardless of the specified value in <see cref="IStreamConfig"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <returns>a <see cref="IGlobalKTable{K,V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IGlobalKTable<K, V> GlobalTable<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            => GlobalTable(topic, keySerdes, valueSerdes, materialized, null, null);

        /// <summary>
        /// Create a <see cref="IGlobalKTable{K,V}"/> for the specified topic.
        /// The default <see cref="ITimestampExtractor"/>, as specified in the <see cref="IStreamConfig"/> is used.
        /// Input keyvalue records with <code>null</code> key will be dropped.
        /// The resulting <see cref="IGlobalKTable{K,V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// However, no internal changelog topic is created since the original input topic can be used for recovery.
        /// Note that <see cref="IGlobalKTable{K,V}"/> always applies <code>"auto.offset.reset"</code> strategy <code>"earliest"</code>
        /// regardless of the specified value in <see cref="IStreamConfig"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <param name="named">Processor name</param>
        /// <returns>a <see cref="IGlobalKTable{K,V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IGlobalKTable<K, V> GlobalTable<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named)
            => GlobalTable(topic, keySerdes, valueSerdes, materialized, named, null);

        /// <summary>
        /// Create a <see cref="IGlobalKTable{K,V}"/> for the specified topic.
        /// Input keyvalue records with <code>null</code> key will be dropped.
        /// The resulting <see cref="IGlobalKTable{K,V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// However, no internal changelog topic is created since the original input topic can be used for recovery.
        /// Note that <see cref="IGlobalKTable{K,V}"/> always applies <code>"auto.offset.reset"</code> strategy <code>"earliest"</code>
        /// regardless of the specified value in <see cref="IStreamConfig"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <param name="named">Processor name</param>
        /// <param name="extractor">the timestamp extractor to be used. If null the default timestamp extractor from config will be used</param>
        /// <returns>a <see cref="IGlobalKTable{K,V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IGlobalKTable<K, V> GlobalTable<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named, ITimestampExtractor extractor)
        {
            materialized = materialized ?? Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.Create();

            var consumedInternal = new ConsumedInternal<K, V>(named, keySerdes, valueSerdes, extractor);
            materialized.UseProvider(internalStreamBuilder, $"{topic}-")?.InitConsumed(consumedInternal);

            return internalStreamBuilder.GlobalTable(topic, consumedInternal, materialized);
        }

        #endregion

        #region GlobalKTable<K, V, KS, VS>

        /// <summary>
        /// Create a <see cref="IGlobalKTable{K,V}"/> for the specified topic.
        /// The default <see cref="ITimestampExtractor"/>, as specified in the <see cref="IStreamConfig"/> is used.
        /// Input keyvalue records with <code>null</code> key will be dropped.
        /// The resulting <see cref="IGlobalKTable{K,V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/>
        /// with an internal store name. Note that store name may not be queriable through Interactive Queries.
        /// No internal changelog topic is created since the original input topic can be used for recovery.
        /// Note that <see cref="IGlobalKTable{K,V}"/> always applies <code>"auto.offset.reset"</code> strategy <code>"earliest"</code>
        /// regardless of the specified value in <see cref="IStreamConfig"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <returns>a <see cref="IGlobalKTable{K,V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IGlobalKTable<K, V> GlobalTable<K, V, KS, VS>(string topic)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => GlobalTable<K, V, KS, VS>(topic, null, null, null);

        /// <summary>
        /// Create a <see cref="IGlobalKTable{K,V}"/> for the specified topic.
        /// The default <see cref="ITimestampExtractor"/>, as specified in the <see cref="IStreamConfig"/> is used.
        /// Input keyvalue records with <code>null</code> key will be dropped.
        /// The resulting <see cref="IGlobalKTable{K,V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// However, no internal changelog topic is created since the original input topic can be used for recovery.
        /// Note that <see cref="IGlobalKTable{K,V}"/> always applies <code>"auto.offset.reset"</code> strategy <code>"earliest"</code>
        /// regardless of the specified value in <see cref="IStreamConfig"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <returns>a <see cref="IGlobalKTable{K,V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IGlobalKTable<K, V> GlobalTable<K, V, KS, VS>(string topic, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => GlobalTable<K, V, KS, VS>(topic, materialized, null, null);

        /// <summary>
        /// Create a <see cref="IGlobalKTable{K,V}"/> for the specified topic.
        /// The default <see cref="ITimestampExtractor"/>, as specified in the <see cref="IStreamConfig"/> is used.
        /// Input keyvalue records with <code>null</code> key will be dropped.
        /// The resulting <see cref="IGlobalKTable{K,V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// However, no internal changelog topic is created since the original input topic can be used for recovery.
        /// Note that <see cref="IGlobalKTable{K,V}"/> always applies <code>"auto.offset.reset"</code> strategy <code>"earliest"</code>
        /// regardless of the specified value in <see cref="IStreamConfig"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <param name="named">Processor name</param>
        /// <returns>a <see cref="IGlobalKTable{K,V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IGlobalKTable<K, V> GlobalTable<K, V, KS, VS>(string topic, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => GlobalTable<K, V, KS, VS>(topic, materialized, named, null);

        /// <summary>
        /// Create a <see cref="IGlobalKTable{K,V}"/> for the specified topic.
        /// Input keyvalue records with <code>null</code> key will be dropped.
        /// The resulting <see cref="IGlobalKTable{K,V}"/> will be materialized in a local <see cref="IKeyValueStore{K, V}"/> using the given
        /// <see cref="Materialized{K, V, S}"/> instance.
        /// However, no internal changelog topic is created since the original input topic can be used for recovery.
        /// Note that <see cref="IGlobalKTable{K,V}"/> always applies <code>"auto.offset.reset"</code> strategy <code>"earliest"</code>
        /// regardless of the specified value in <see cref="IStreamConfig"/>.
        /// </summary>
        /// <typeparam name="K">Key type of record</typeparam>
        /// <typeparam name="V">Value type of record</typeparam>
        /// <typeparam name="KS">Key deserializer type</typeparam>
        /// <typeparam name="VS">Value deserializer type</typeparam>
        /// <param name="topic">the topic name, can't be null</param>
        /// <param name="materialized">the instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store.</param>
        /// <param name="named">Processor name</param>
        /// <param name="extractor">the timestamp extractor to be used. If null the default timestamp extractor from config will be used</param>
        /// <returns>a <see cref="IGlobalKTable{K,V}"/> for the specified topic</returns>
        /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if topic is null or empty</exception>
        public IGlobalKTable<K, V> GlobalTable<K, V, KS, VS>(string topic, Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named, ITimestampExtractor extractor)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => GlobalTable(topic, new KS(), new VS(), materialized, named, extractor);


        #endregion

        #endregion

        #region State Store

        /// <summary>
        /// Adds a state store to the underlying <see cref="Topology"/>. 
        /// It is required to connect state stores to <see cref="Streamiz.Kafka.Net.Processors.Public.IProcessor{K, V}"/> 
        /// or <see cref="Streamiz.Kafka.Net.Processors.Public.ITransformer{K, V, K1, V1}"/>
        /// before they can be used.
        /// </summary>
        /// <param name="storeBuilder">The builder used to obtain the <see cref="IStateStore"/> instance.</param>
        public void AddStateStore(IStoreBuilder storeBuilder)
        {
            internalStreamBuilder.AddStateStore(storeBuilder);
        }

        #endregion

        /// <summary>
        /// Returns the <see cref="Topology"/> that represents the specified processing logic.
        /// Note that using this method means no optimizations are performed.
        /// </summary>
        /// <returns>the <see cref="Topology"/> that represents the specified processing logic</returns>
        public Topology Build()
        {
            internalStreamBuilder.Build();
            return topology;
        }
    }
}
