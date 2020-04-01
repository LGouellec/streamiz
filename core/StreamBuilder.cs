using kafka_stream_core.Crosscutting;
using kafka_stream_core.Processors.Internal;
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

        public IKStream<byte[], byte[]> stream(string topic) 
            => stream(topic, Consumed<byte[], byte[]>.Create());

        public IKStream<K, V> stream<K,V>(string topic, Consumed<K, V> consumed)
        {
            return internalStreamBuilder.Stream<K, V>(topic, consumed);
        }

        #endregion

        #region KTable
        
        public IKTable<byte[], byte[]> table(string topic)
            => table(topic, Consumed<byte[], byte[]>.Create(), Materialized<byte[], byte[], KeyValueStore<Bytes, byte[]>>.Create());

        public IKTable<K,V> table<K,V>(string topic, Consumed<K,V> consumed, Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized = null)
        {
            materialized?.UseProvider(internalStreamBuilder, $"{topic}-").InitConsumed(consumed);

            return internalStreamBuilder.Table(topic, consumed, materialized);
        }

        #endregion

        public Topology Build()
        {
            this.internalStreamBuilder.Build();
            return topology;
        }
    }
}
