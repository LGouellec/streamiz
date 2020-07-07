using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class StreamJoinBuilder
    {
        private readonly InternalStreamBuilder builder;
        private readonly bool leftOuter;
        private readonly bool rightOuter;

        public StreamJoinBuilder(InternalStreamBuilder builder, bool leftOuter, bool rightOuter)
        {
            this.builder = builder;
            this.leftOuter = leftOuter;
            this.rightOuter = rightOuter;
        }

        public KStream<K, VR> Join<K, V, V0, VR>(
            KStream<K, V> joinLeft,
            KStream<K, V0> joinRight,
            IValueJoiner<V, V0, VR> joiner,
            JoinWindowOptions windows,
            Joined<K, V, V0> joined)
        {
            var named = new Named(joined.Name);
            var joinThisSuffix = rightOuter ? "-outer-this-join" : "-this-join";
            var joinOtherSuffix = leftOuter ? "-outer-other-join" : "-other-join";

            var thisWindowStreamProcessorName = named.SuffixWithOrElseGet("-this-windowed", builder, KStream.WINDOWED_NAME);
            var otherWindowStreamProcessorName = named.SuffixWithOrElseGet("-other-windowed", builder, KStream.WINDOWED_NAME);

            var joinThisGeneratedName = rightOuter ? builder.NewProcessorName(KStream.OUTERTHIS_NAME) : builder.NewProcessorName(KStream.JOINTHIS_NAME);
            var joinOtherGeneratedName = leftOuter ? builder.NewProcessorName(KStream.OUTEROTHER_NAME) : builder.NewProcessorName(KStream.JOINOTHER_NAME);

            var joinThisName = named.SuffixWithOrElseGet(joinThisSuffix, joinThisGeneratedName);
            var joinOtherName = named.SuffixWithOrElseGet(joinOtherSuffix, joinOtherGeneratedName);

            var joinMergeName = named.SuffixWithOrElseGet("-merge", builder, KStream.MERGE_NAME);

            StreamGraphNode thisStreamsGraphNode = joinLeft.Node;
            StreamGraphNode otherStreamsGraphNode = joinRight.Node;
            var userProvidedBaseStoreName = joined.Name; // TODO : renamed, and create a StreamJoined DTO

            // TODO TO FINISH

            return null;
        }
    }
}
