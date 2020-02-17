using kafka_stream_core.Processors.Internal;
using kafka_stream_core.SerDes;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal;

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

        public KStream<string, string> stream(string topic) => this.stream(topic, Consumed<string, string>.with(new StringSerDes(), new StringSerDes()));

        public KStream<K, V> stream<K,V>(string topic, Consumed<K, V> consumed)
        {
            return internalStreamBuilder.stream(topic, consumed);
        }

        public Topology build()
        {
            this.internalStreamBuilder.build();
            return topology;
        }
    }
}
