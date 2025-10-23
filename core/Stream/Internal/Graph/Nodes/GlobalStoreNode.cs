using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class GlobalStoreNode<K, V, S> : StateStoreNode
        where S : IStateStore
    {
        private readonly IStoreBuilder<S> _storeBuilder;
        private readonly string _topic;
        private readonly ConsumedInternal<K, V> _consumed;
        private readonly ProcessorSupplier<K, V> _stateUpdateSupplier;
        private readonly bool _reprocessOnRestore;

        public GlobalStoreNode(
            IStoreBuilder<S> storeBuilder,
            string sourceName,
            string processorName,
            string topic, 
            ConsumedInternal<K, V> consumed,
            ProcessorSupplier<K, V> stateUpdateSupplier,
            bool reprocessOnRestore)
            : base(storeBuilder, sourceName, processorName)
        {
            _storeBuilder = storeBuilder;
            _topic = topic;
            _consumed = consumed;
            _stateUpdateSupplier = stateUpdateSupplier;
            _reprocessOnRestore = reprocessOnRestore;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            _storeBuilder.WithLoggingDisabled();
            builder.AddGlobalStore(_topic,
                _storeBuilder,
                streamGraphNode,
                _consumed,
                new ProcessorParameters<K, V>(new KStreamProcessorSupplier<K, V>(_stateUpdateSupplier),
                    processorNodeNames[0]),
                _reprocessOnRestore);

        }
    }
}