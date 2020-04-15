using kafka_stream_core.Crosscutting;
using kafka_stream_core.Processors;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.SerDes;
using kafka_stream_core.State;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal;
using kafka_stream_core.Table;

namespace kafka_stream_core
{
    public class StreamBuilder
    {
        private readonly Topology topology = new Topology();
        private readonly InternalTopologyBuilder internalTopologyBuilder;
        private readonly InternalStreamBuilder internalStreamBuilder;

        public StreamBuilder()
        {
            internalTopologyBuilder = topology.Builder;
            internalStreamBuilder = new InternalStreamBuilder(internalTopologyBuilder);
        }

        #region KStream

        public IKStream<byte[], byte[]> Stream(string topic, StreamOptions options = null) 
            => Stream<byte[], byte[], ByteArraySerDes, ByteArraySerDes>(topic, options);

        public IKStream<K, V> Stream<K, V>(string topic, StreamOptions options = null)
            => Stream<K, V>(topic, null, null, options);

        public IKStream<K, V> Stream<K, V, KS, VS>(string topic, StreamOptions options = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Stream(topic, new KS(), new VS(), options);

        public IKStream<K, V> Stream<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, StreamOptions options = null)
        {
            var consumedInternal = new ConsumedInternal<K, V>(options?.Named, keySerdes, valueSerdes, options?.Extractor);
            return internalStreamBuilder.Stream(topic, consumedInternal);
        }

        #endregion

        #region KTable

        public IKTable<byte[], byte[]> Table(string topic, StreamOptions options = null)
            => Table<byte[], byte[], ByteArraySerDes, ByteArraySerDes>(topic, options, Materialized<byte[], byte[], KeyValueStore<Bytes, byte[]>>.Create());

        public IKTable<K,V> Table<K,V>(string topic, StreamOptions options = null, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized = null)
        {
            materialized = materialized == null ? Materialized<K, V, KeyValueStore<Bytes, byte[]>>.Create() : materialized;

            var consumedInternal = new ConsumedInternal<K, V>(options?.Named, null, null, options?.Extractor);
            materialized?.UseProvider(internalStreamBuilder, $"{topic}-")?.InitConsumed(consumedInternal);

            return internalStreamBuilder.Table(topic, consumedInternal, materialized);
        }

        public IKTable<K, V> Table<K, V, KS, VS>(string topic, StreamOptions options = null, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            materialized = materialized == null ? Materialized<K, V, KeyValueStore<Bytes, byte[]>>.Create<KS, VS>() : materialized;

            var consumedInternal = new ConsumedInternal<K, V>(options?.Named, new KS(), new VS(), options?.Extractor);
            materialized?.UseProvider(internalStreamBuilder, $"{topic}-")?.InitConsumed(consumedInternal);

            return internalStreamBuilder.Table(topic, consumedInternal, materialized);
        }

        public IKTable<K, V> Table<K, V>(string topic, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, StreamOptions options = null, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized = null)
        {
            return null;
        }

        #endregion

        public Topology Build()
        {
            this.internalStreamBuilder.Build();
            return topology;
        }
    }
}
