using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Table;

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

        #region KStream byte[]

        public IKStream<byte[], byte[]> Stream(string topic) 
            => Stream<byte[], byte[], ByteArraySerDes, ByteArraySerDes>(topic);

        public IKStream<byte[], byte[]> Stream(string topic, string named)
            => Stream<byte[], byte[], ByteArraySerDes, ByteArraySerDes>(topic);
        public IKStream<byte[], byte[]> Stream(string topic, ITimestampExtractor extractor)
            => Stream<byte[], byte[], ByteArraySerDes, ByteArraySerDes>(topic);
        public IKStream<byte[], byte[]> Stream(string topic, string named, ITimestampExtractor extractor)
            => Stream<byte[], byte[], ByteArraySerDes, ByteArraySerDes>(topic);

        #endregion

        #region KStream<K, V>

        public IKStream<K, V> Stream<K, V>(string topic)
            => Stream<K, V>(topic, null, null);

        public IKStream<K, V> Stream<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            => this.Stream(topic, keySerdes, valueSerdes, null, null);

        public IKStream<K, V> Stream<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, string named)
            => this.Stream(topic, keySerdes, valueSerdes, named, null);

        public IKStream<K, V> Stream<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, ITimestampExtractor extractor)
            => this.Stream(topic, keySerdes, valueSerdes, null, extractor);

        public IKStream<K, V> Stream<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, string named, ITimestampExtractor extractor)
        {
            var consumedInternal = new ConsumedInternal<K, V>(named, keySerdes, valueSerdes, extractor);
            return internalStreamBuilder.Stream(topic, consumedInternal);
        }

        #endregion

        #region KStream<K, V, KS, VS>

        public IKStream<K, V> Stream<K, V, KS, VS>(string topic)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Stream<K, V, KS, VS>(topic, null, null);

        public IKStream<K, V> Stream<K, V, KS, VS>(string topic, string named)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Stream<K, V, KS, VS>(topic, named, null);

        public IKStream<K, V> Stream<K, V, KS, VS>(string topic, ITimestampExtractor extractor)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Stream<K, V, KS, VS>(topic, null, extractor);

        public IKStream<K, V> Stream<K, V, KS, VS>(string topic, string named, ITimestampExtractor extractor)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Stream(topic, new KS(), new VS(), named, extractor);

        #endregion

        #endregion

        #region KTable

        public IKTable<byte[], byte[]> Table(string topic, StreamOptions options = null)
            => Table<byte[], byte[], ByteArraySerDes, ByteArraySerDes>(topic, options, Materialized<byte[], byte[], KeyValueStore<Bytes, byte[]>>.Create());

        public IKTable<K,V> Table<K,V>(string topic, StreamOptions options = null, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized = null)
        {
            materialized = materialized ?? Materialized<K, V, KeyValueStore<Bytes, byte[]>>.Create();

            var consumedInternal = new ConsumedInternal<K, V>(options?.Named, null, null, options?.Extractor);
            materialized?.UseProvider(internalStreamBuilder, $"{topic}-")?.InitConsumed(consumedInternal);

            return internalStreamBuilder.Table(topic, consumedInternal, materialized);
        }

        public IKTable<K, V> Table<K, V, KS, VS>(string topic, StreamOptions options = null, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            materialized = materialized ?? Materialized<K, V, KeyValueStore<Bytes, byte[]>>.Create<KS, VS>();

            var consumedInternal = new ConsumedInternal<K, V>(options?.Named, new KS(), new VS(), options?.Extractor);
            materialized?.UseProvider(internalStreamBuilder, $"{topic}-")?.InitConsumed(consumedInternal);

            return internalStreamBuilder.Table(topic, consumedInternal, materialized);
        }

        public IKTable<K, V> Table<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, StreamOptions options = null, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized = null)
        {
            return null;
        }

        #endregion

        /// <summary>
        /// Returns the <see cref="Topology"/> that represents the specified processing logic.
        /// Note that using this method means no optimizations are performed.
        /// </summary>
        /// <returns>the <see cref="Topology"/> that represents the specified processing logic</returns>
        public Topology Build()
        {
            this.internalStreamBuilder.Build();
            return topology;
        }
    }
}
