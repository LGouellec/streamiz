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

        public IKStream<byte[], byte[]> Stream(string topic, StreamOptions options = null) => Stream<byte[], byte[], ByteArraySerDes, ByteArraySerDes>(topic, options);

        public IKStream<K, V> Stream<K, V>(string topic, StreamOptions options = null)
        {
            var consumedInternal = new ConsumedInternal<K, V>(null, null, options?.Extractor);
            return internalStreamBuilder.Stream(topic, consumedInternal);
        }

        public IKStream<K, V> Stream<K, V, KS, VS>(string topic, StreamOptions options = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            var consumedInternal = new ConsumedInternal<K, V>(new KS(), new VS(), options?.Extractor);
            return internalStreamBuilder.Stream(topic, consumedInternal);
        }

        #endregion

        #region KTable

        public IKTable<byte[], byte[]> Table(string topic, StreamOptions options = null)
            => Table<byte[], byte[], ByteArraySerDes, ByteArraySerDes>(topic, options, Materialized<byte[], byte[], KeyValueStore<Bytes, byte[]>>.Create());

        public IKTable<K, V> Table<K, V>(string topic, StreamOptions options = null)
            => Table(topic, options, Materialized<K, V, KeyValueStore<Bytes, byte[]>>.Create());

        public IKTable<K,V> Table<K,V>(string topic, StreamOptions options = null, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized = null)
        {
            var consumedInternal = new ConsumedInternal<K, V>(null, null, options?.Extractor);
            materialized?.UseProvider(internalStreamBuilder, $"{topic}-")?.InitConsumed(consumedInternal);

            return internalStreamBuilder.Table(topic, consumedInternal, materialized);
        }

        public IKTable<K, V> Table<K, V, KS, VS>(string topic, StreamOptions options = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Table<K, V, KS, VS>(topic, options, Materialized<K, V, KeyValueStore<Bytes, byte[]>>.Create<KS, VS>());

        public IKTable<K, V> Table<K, V, KS, VS>(string topic, StreamOptions options = null, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            var consumedInternal = new ConsumedInternal<K, V>(new KS(), new VS(), options?.Extractor);
            materialized?.UseProvider(internalStreamBuilder, $"{topic}-")?.InitConsumed(consumedInternal);

            return internalStreamBuilder.Table(topic, consumedInternal, materialized);
        }

        #endregion

        public Topology Build()
        {
            this.internalStreamBuilder.Build();
            return topology;
        }
    }
}
