using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class InternalTopologyBuilder
    {
        private readonly IDictionary<string, NodeFactory> nodeFactories = new Dictionary<string, NodeFactory>();
        private readonly IDictionary<string, StateStoreFactory> stateFactories = new Dictionary<string, StateStoreFactory>();
        private readonly IList<string> sourcesTopics = new List<string>();
        private readonly QuickUnion<string> nodeGrouper = new QuickUnion<string>();
        private IDictionary<string, ISet<string>> nodeGroups = new Dictionary<string, ISet<string>>();

        internal InternalTopologyBuilder()
        {
        }

        internal IEnumerable<string> GetSourceTopics() => sourcesTopics;

        #region Private

        private void ConnectProcessorAndStateStore(string processorName, string stateStoreName)
        {
            if (!stateFactories.ContainsKey(stateStoreName))
            {
                throw new TopologyException("StateStore " + stateStoreName + " is not added yet.");
            }
            if (!nodeFactories.ContainsKey(processorName))
            {
                throw new TopologyException("Processor " + processorName + " is not added yet.");
            }

            var nodeFactory = nodeFactories[processorName];

            if (nodeFactory is IProcessorNodeFactory)
                ((IProcessorNodeFactory)nodeFactory).AddStateStore(stateStoreName);
            else
                throw new TopologyException($"Cannot connect a state store {stateStoreName} to a source node or a sink node.");
        }

        #endregion

        #region Add Processors / State Store

        internal void AddSourceOperator<K, V>(string topic, string nameNode, ConsumedInternal<K, V> consumed)
        {
            if (string.IsNullOrEmpty(topic))
                throw new TopologyException("You must provide at least one topic");

            if (nodeFactories.ContainsKey(nameNode))
                throw new TopologyException($"Source processor {nameNode} is already added.");

            if (sourcesTopics.Contains(topic))
            {
                throw new TopologyException($"Topic {topic} has already been registered by another source.");
            }

            sourcesTopics.Add(topic);
            nodeFactories.Add(nameNode,
                new SourceNodeFactory<K, V>(nameNode, topic, consumed.TimestampExtractor, consumed.KeySerdes, consumed.ValueSerdes));
            nodeGrouper.Add(nameNode);
            nodeGroups = null;
        }

        internal void AddSinkOperator<K, V>(ITopicNameExtractor<K, V> topicNameExtractor, string nameNode, Produced<K, V> produced, params string[] previousProcessorNames)
        {
            if (nodeFactories.ContainsKey(nameNode))
                throw new TopologyException($"Sink processor {nameNode} is already added.");

            nodeFactories.Add(nameNode,
                new SinkNodeFactory<K, V>(nameNode, previousProcessorNames, topicNameExtractor, produced.KeySerdes, produced.ValueSerdes));
            nodeGrouper.Add(nameNode);
            nodeGrouper.Unite(nameNode, previousProcessorNames);
            nodeGroups = null;
        }

        internal void AddProcessor<K, V>(string nameNode, IProcessorSupplier<K, V> processor, params string[] previousProcessorNames)
        {
            if (nodeFactories.ContainsKey(nameNode))
                throw new TopologyException($"Processor {nameNode} is already added.");

            nodeFactories.Add(nameNode, new ProcessorNodeFactory<K, V>(nameNode, previousProcessorNames, processor));
            nodeGrouper.Add(nameNode);
            nodeGrouper.Unite(nameNode, previousProcessorNames);
            nodeGroups = null;
        }

        internal void AddStateStore<S>(StoreBuilder<S> storeBuilder, params string[] processorNames)
            where S : IStateStore
        {
            AddStateStore<S>(storeBuilder, false, processorNames);
        }

        internal void AddStateStore<S>(StoreBuilder<S> storeBuilder, bool allowOverride, params string[] processorNames)
            where S : IStateStore
        {
            if (!allowOverride && stateFactories.ContainsKey(storeBuilder.Name))
            {
                throw new TopologyException("StateStore " + storeBuilder.Name + " is already added.");
            }

            stateFactories.Add(storeBuilder.Name, new StateStoreFactory(storeBuilder));

            if (processorNames != null)
            {
                foreach (var processorName in processorNames)
                {
                    ConnectProcessorAndStateStore(processorName, storeBuilder.Name);
                }
            }
        }

        #endregion

        #region Build

        public ProcessorTopology BuildTopology() => BuildTopology((string)null);

        public ProcessorTopology BuildTopology(string topic)
        {
            ISet<string> nodeGroup = null;
            if (!string.IsNullOrEmpty(topic))
            {
                var groups = NodeGroups();
                if (groups.ContainsKey(topic))
                    nodeGroup = NodeGroups()[topic];
                else
                    throw new TopologyException($"Topic {topic} doesn't exist in this topology");
            }
            else
                nodeGroup = NodeGroups().Values.SelectMany(i => i).ToHashSet();

            return BuildTopology(nodeGroup);
        }

        private ProcessorTopology BuildTopology(ISet<string> nodeGroup)
        {
            IProcessor rootProcessor = new RootProcessor();
            IDictionary<string, IProcessor> sources = new Dictionary<string, IProcessor>();
            IDictionary<string, IProcessor> sinks = new Dictionary<string, IProcessor>();
            IDictionary<string, IProcessor> processors = new Dictionary<string, IProcessor>();
            IDictionary<string, IStateStore> stateStores = new Dictionary<string, IStateStore>();

            foreach (var nodeFactory in nodeFactories.Values)
            {
                if (nodeGroup == null || nodeGroup.Contains(nodeFactory.Name))
                {
                    var processor = nodeFactory.Build();
                    processors.Add(nodeFactory.Name, processor);

                    if (nodeFactory is IProcessorNodeFactory)
                        BuildProcessorNode(processors, stateStores, nodeFactory as IProcessorNodeFactory, processor);
                    else if (nodeFactory is ISourceNodeFactory)
                        BuildSourceNode(sources, nodeFactory as ISourceNodeFactory, processor);
                    else if (nodeFactory is ISinkNodeFactory)
                        BuildSinkNode(processors, sinks, nodeFactory as ISinkNodeFactory, processor);
                    else
                        throw new TopologyException($"Unknown definition class: {nodeFactory.GetType().Name}");
                }
            }

            foreach (var sourceProcessor in sources.Values)
                rootProcessor.AddNextProcessor(sourceProcessor);

            return new ProcessorTopology(rootProcessor, sources, sinks, processors, stateStores);
        }

        private void BuildSinkNode(IDictionary<string, IProcessor> processors, IDictionary<string, IProcessor> sinks, ISinkNodeFactory factory, IProcessor processor)
        {
            foreach (var predecessor in factory.Previous)
            {
                processors[predecessor].AddNextProcessor(processor);
            }

            sinks.Add(factory.Name, processor);
        }

        private void BuildSourceNode(IDictionary<string, IProcessor> sources, ISourceNodeFactory factory, IProcessor processor)
        {
            sources.Add(factory.Name, processor);
        }

        private void BuildProcessorNode(IDictionary<string, IProcessor> processors, IDictionary<string, IStateStore> stateStores, IProcessorNodeFactory factory, IProcessor processor)
        {
            foreach (string predecessor in factory.Previous)
            {
                IProcessor predecessorNode = processors[predecessor];
                predecessorNode.AddNextProcessor(processor);
            }

            foreach (string stateStoreName in factory.StateStores)
            {
                if (!stateStores.ContainsKey(stateStoreName) && stateFactories.ContainsKey(stateStoreName))
                {
                    StateStoreFactory stateStoreFactory = stateFactories[stateStoreName];

                    // TODO : changelog topic (remember the changelog topic if this state store is change-logging enabled)
                    stateStores.Add(stateStoreName, stateStoreFactory.Build());
                }
            }
        }

        internal void RewriteTopology(IStreamConfig config)
        {
            // NOTHING FOR MOMENT
        }

        internal void BuildAndOptimizeTopology(RootNode root, IList<StreamGraphNode> nodes)
        {
            foreach (var node in nodes)
            {
                if (node.AllParentsWrittenToTopology && !node.HasWrittenToTopology)
                {
                    node.WriteToTopology(this);
                    node.HasWrittenToTopology = true;
                }
            }
        }

        #endregion

        #region Make Groups

        internal IDictionary<string, ISet<string>> NodeGroups()
        {
            if (nodeGroups == null)
            {
                nodeGroups = MakeNodeGroups();
            }

            return nodeGroups;
        }

        private IDictionary<string, ISet<string>> MakeNodeGroups()
        {
            IDictionary<string, ISet<string>> groups = new Dictionary<string, ISet<string>>();

            foreach (var topicSource in sourcesTopics)
            {
                groups.Add(topicSource, new HashSet<string>());
                PutNodeGroupName(groups, topicSource);
            }

            return groups;
        }

        private void PutNodeGroupName(IDictionary<string, ISet<string>> rootToNodeGroup, string topicSource)
        {
            var sourceNode = nodeFactories.Values.FirstOrDefault(n => n is ISourceNodeFactory && (n as ISourceNodeFactory).Topic.Equals(topicSource)) as ISourceNodeFactory;
            if (sourceNode != null)
            {
                IList<string> nodes = new List<string>();
                foreach (var v in nodeGrouper.Ids)
                    if (v.Value.Equals(sourceNode.Name))
                        nodes.Add(v.Key);

                rootToNodeGroup[topicSource].AddRange(nodes);
            }
        }

        #endregion

        #region Describe

        internal ITopologyDescription Describe()
        {
            var topologyDes = new TopologyDescription();

            foreach (var kp in NodeGroups())
                DescribeSubTopology(topologyDes, kp.Key, kp.Value);

            return topologyDes;
        }

        private void DescribeSubTopology(TopologyDescription description, string key, ISet<string> values)
        {
            IDictionary<string, NodeDescription> nodesByName = new Dictionary<string, NodeDescription>();
            foreach (var name in values)
                nodesByName.Add(name, nodeFactories[name].Describe());

            foreach (var node in nodesByName.Values)
            {
                foreach (var prev in nodeFactories[node.Name].Previous)
                {
                    var prevNode = nodesByName[prev];
                    node.AddPredecessor(prevNode);
                    prevNode.AddSuccessor(node);
                }
            }

            description.AddSubtopology(new SubTopologyDescription(key, nodesByName.Values.ToList<INodeDescription>()));
        }

        #endregion
    }
}