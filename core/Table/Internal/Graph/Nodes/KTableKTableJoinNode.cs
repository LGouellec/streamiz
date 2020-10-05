using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System;

namespace Streamiz.Kafka.Net.Table.Internal.Graph.Nodes
{
    internal class KTableKTableJoinNode<K, V1, V2, VR> :
        BaseJoinProcessorNode<K, Change<V1>, Change<V2>, Change<VR>>
    {
        private readonly string[] leftStoreNames;
        private readonly string[] rightStoreNames;
        private readonly string queryableStoreName;
        private readonly StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder;

        public KTableKTableJoinNode(
            string name,
            IValueJoiner<Change<V1>, Change<V2>, Change<VR>> valueJoiner,
            ProcessorParameters<K, Change<V1>> joinLeftParams,
            ProcessorParameters<K, Change<V2>> joinRightParams,
            ProcessorParameters<K, Change<VR>> joinMergeParams,
            string leftJoinSideName,
            string rightJoinSideName,
            string[] leftStoreNames,
            string[] rightStoreNames,
            string queryableStoreName,
            StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder)
            : base(name, valueJoiner, joinLeftParams, joinRightParams, joinMergeParams, leftJoinSideName, rightJoinSideName)
        {
            this.leftStoreNames = leftStoreNames;
            this.rightStoreNames = rightStoreNames;
            this.queryableStoreName = queryableStoreName;
            this.storeBuilder = storeBuilder;

            JoinMergeProcessorSupplier = new KTableKTableJoinMerger<K, V1, V2, VR>(
                (IKTableProcessorSupplier<K, V1, VR>)joinLeftParams.Processor,
                (IKTableProcessorSupplier<K, V2, VR>)joinRightParams.Processor,
                queryableStoreName);
        }

        public IProcessorSupplier<K, Change<VR>> JoinMergeProcessorSupplier { get; }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            throw new NotImplementedException();
        }
    }
}
