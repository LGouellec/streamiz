using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Stream.Internal.Graph;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Linq;

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
            StreamJoinProps<K, V, V0> joined,
            ISerDes<VR> otherValueSerdes)
        {
            var named = new Named(joined.Name);
            var joinLeftSuffix = rightOuter ? "-outer-this-join" : "-this-join";
            var joinRightSuffix = leftOuter ? "-outer-other-join" : "-other-join";

            var leftWindowStreamProcessorName = named.SuffixWithOrElseGet("-this-windowed", builder, KStream.WINDOWED_NAME);
            var rightWindowStreamProcessorName = named.SuffixWithOrElseGet("-other-windowed", builder, KStream.WINDOWED_NAME);

            var joinLeftGeneratedName = rightOuter ? builder.NewProcessorName(KStream.OUTERTHIS_NAME) : builder.NewProcessorName(KStream.JOINTHIS_NAME);
            var joinRightGeneratedName = leftOuter ? builder.NewProcessorName(KStream.OUTEROTHER_NAME) : builder.NewProcessorName(KStream.JOINOTHER_NAME);

            var joinLeftName = named.SuffixWithOrElseGet(joinLeftSuffix, joinLeftGeneratedName);
            var joinRightName = named.SuffixWithOrElseGet(joinRightSuffix, joinRightGeneratedName);

            var joinMergeName = named.SuffixWithOrElseGet("-merge", builder, KStream.MERGE_NAME);

            StreamGraphNode leftStreamsGraphNode = joinLeft.Node;
            StreamGraphNode rightStreamsGraphNode = joinRight.Node;
            var userProvidedBaseStoreName = joined.StoreName;

            var leftStoreSupplier = joined.LeftStoreSupplier;
            var rightStoreSupplier = joined.RightStoreSupplier;

            IStoreBuilder<IWindowStore<K, V>> leftWindowStore;
            IStoreBuilder<IWindowStore<K, V0>> rightWindowStore;

            AssertUniqueStoreNames(leftStoreSupplier, rightStoreSupplier);

            if (leftStoreSupplier == null)
            {
                var thisJoinStoreName = userProvidedBaseStoreName == null ? joinLeftGeneratedName : userProvidedBaseStoreName + joinLeftSuffix;
                leftWindowStore = JoinWindowStoreBuilder(thisJoinStoreName, windows, joined.KeySerdes, joined.LeftValueSerdes);
            }
            else
            {
                AssertWindowSettings(leftStoreSupplier, windows);
                leftWindowStore = Stores.WindowStoreBuilder(leftStoreSupplier, joined.KeySerdes, joined.LeftValueSerdes);
            }

            if (rightStoreSupplier == null)
            {
                var otherJoinStoreName = userProvidedBaseStoreName == null ? joinRightGeneratedName : userProvidedBaseStoreName + joinRightSuffix;
                rightWindowStore = JoinWindowStoreBuilder(otherJoinStoreName, windows, joined.KeySerdes, joined.RightValueSerdes);
            }
            else
            {
                AssertWindowSettings(rightStoreSupplier, windows);
                rightWindowStore = Stores.WindowStoreBuilder(rightStoreSupplier, joined.KeySerdes, joined.RightValueSerdes);
            }


            var leftStream = new KStreamJoinWindow<K, V>(leftWindowStore.Name);
            var leftStreamParams = new ProcessorParameters<K, V>(leftStream, leftWindowStreamProcessorName);
            var leftNode = new ProcessorGraphNode<K, V>(leftWindowStreamProcessorName, leftStreamParams);
            builder.AddGraphNode(leftStreamsGraphNode, leftNode);

            var rightStream = new KStreamJoinWindow<K, V0>(rightWindowStore.Name);
            var rightStreamParams = new ProcessorParameters<K, V0>(rightStream, rightWindowStreamProcessorName);
            var rightNode = new ProcessorGraphNode<K, V0>(rightWindowStreamProcessorName, rightStreamParams);
            builder.AddGraphNode(rightStreamsGraphNode, rightNode);

            var joinL = new KStreamKStreamJoin<K, V, V0, VR>(joinLeftName, rightWindowStore.Name, windows.beforeMs, windows.afterMs, joiner, leftOuter);
            var joinLParams = new ProcessorParameters<K, V>(joinL, joinLeftName);

            var joinR = new KStreamKStreamJoin<K, V0, V, VR>(joinRightName, leftWindowStore.Name, windows.afterMs, windows.beforeMs, joiner.Reverse(), rightOuter);
            var joinRParams = new ProcessorParameters<K, V0>(joinR, joinRightName);
            var merge = new PassThrough<K, VR>();
            var mergeParams = new ProcessorParameters<K, VR>(merge, joinMergeName);

            var joinNode = new StreamStreamJoinNode<K, V, V0, VR>(
                joinMergeName,
                joiner,
                joinLParams,
                joinRParams,
                mergeParams,
                leftStreamParams,
                rightStreamParams,
                leftWindowStore,
                rightWindowStore,
                joined);

            builder.AddGraphNode(new List<StreamGraphNode> { leftStreamsGraphNode, rightStreamsGraphNode }, joinNode);

            ISet<string> allSourceNodes = new HashSet<string>(joinLeft.SetSourceNodes);
            allSourceNodes.AddRange(joinRight.SetSourceNodes);

            return new KStream<K, VR>(
                joinMergeName,
                joined.KeySerdes,
                otherValueSerdes,
                allSourceNodes.ToList(),
                joinNode,
                builder);
        }

        #region Private

        private void AssertWindowSettings(IWindowBytesStoreSupplier supplier, JoinWindowOptions joinWindows)
        {
            bool allMatch = supplier.Retention == (joinWindows.Size + joinWindows.GracePeriodMs) &&
                supplier.WindowSize == joinWindows.Size;

            if (!allMatch)
            {
                throw new StreamsException($"Window settings mismatch. WindowBytesStoreSupplier settings {supplier} supplier must match JoinWindows settings {joinWindows}");
            }
        }

        private void AssertUniqueStoreNames(IWindowBytesStoreSupplier supplier, IWindowBytesStoreSupplier otherSupplier)
        {
            if (supplier != null
                && otherSupplier != null
                && supplier.Name.Equals(otherSupplier.Name))
            {
                throw new StreamsException("Both StoreSuppliers have the same name.  StoreSuppliers must provide unique names");
            }
        }

        private IStoreBuilder<IWindowStore<K, V>> JoinWindowStoreBuilder<K, V>(string storeName,
                                                                             JoinWindowOptions windows,
                                                                             ISerDes<K> keySerde,
                                                                             ISerDes<V> valueSerde)
        {
            return Stores.WindowStoreBuilder(
                Stores.DefaultWindowStore(
                    storeName + "-store",
                    TimeSpan.FromMilliseconds(windows.Size + windows.GracePeriodMs),
                    TimeSpan.FromMilliseconds(windows.Size),
                    Math.Max(windows.Size + windows.GracePeriodMs / 2, 60_000L),
                    true
                ),
                keySerde,
                valueSerde
            );
        }
        #endregion
    }
}
