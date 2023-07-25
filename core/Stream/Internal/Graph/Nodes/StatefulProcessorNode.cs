using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class StatefulProcessorNode<K, V> : ProcessorGraphNode<K, V>
    {
        private readonly string[] storeNames;
        private readonly IStoreBuilder storeBuilder;

        /// <summary>
        /// Create a node representing a stateful processor,
        /// where the store needs to be built and registered as part of building this node.
        /// </summary>
        /// <param name="nameNode"></param>
        /// <param name="parameters"></param>
        /// <param name="storeBuilder"></param>
        /// <param name="storeNames">The names of the state stores used by the processor.</param>

        public StatefulProcessorNode(string nameNode, ProcessorParameters<K, V> parameters, IStoreBuilder storeBuilder, params string[] storeNames)
            : base(nameNode, parameters)
        {
            this.storeNames = storeNames;
            this.storeBuilder = storeBuilder;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            builder.AddProcessor(ProcessorParameters.ProcessorName, ProcessorParameters.Processor, ParentNodeNames());

            if (storeNames != null && storeNames.Length > 0)
            {
                builder.ConnectProcessorAndStateStore(ProcessorParameters.ProcessorName, storeNames);
            }

            if (storeBuilder != null)
            {
                builder.AddStateStore(storeBuilder, ProcessorParameters.ProcessorName);
            }
        }
    }
}
