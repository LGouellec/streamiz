using kafka_stream_core.Nodes.Parameters;
using kafka_stream_core.SerDes;
using kafka_stream_core.Stream;

namespace kafka_stream_core
{
    public class StreamBuilder
    {
        private readonly InternalTopologyBuilder internalTopologyBuilder;
        private readonly InternalStreamBuilder internalStreamBuilder;

        public StreamBuilder()
        {
            internalTopologyBuilder = new InternalTopologyBuilder();
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
            return this.internalTopologyBuilder.build();
        }
    }
}
