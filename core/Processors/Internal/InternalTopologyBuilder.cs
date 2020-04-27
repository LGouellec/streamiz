using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class InternalTopologyBuilder
    {
        private final Map<String, NodeFactory> nodeFactories = new LinkedHashMap<>();

        private IDictionary<string, IProcessor> sourceOperators = new Dictionary<string, IProcessor>();
        private IDictionary<string, IProcessor> sinkOperators = new Dictionary<string, IProcessor>();
        private IDictionary<string, IProcessor> processorOperators = new Dictionary<string, IProcessor>();
        private IDictionary<string, IStateStore> stateStores = new Dictionary<string, IStateStore>();

        private IDictionary<string, StoreBuilder> storeBuilders = new Dictionary<string, StoreBuilder>();

        private IProcessor root = null;

        internal InternalTopologyBuilder()
        {
        }

        public ProcessorTopology BuildTopology()
        {
            var topology = new ProcessorTopology(root, sourceOperators, sinkOperators, processorOperators, stateStores);
            return topology;
        }

        internal IEnumerable<string> GetSourceTopics() => sourceOperators.Values.Select(o => (o as ISourceProcessor).TopicName);

        internal void BuildAndOptimizeTopology(RootNode root, IList<StreamGraphNode> nodes)
        {
            if (this.root == null)
            {
                foreach (var node in nodes)
                {
                    if (node.AllParentsWrittenToTopology && !node.HasWrittenToTopology)
                    {
                        node.WriteToTopology(this);
                        node.HasWrittenToTopology = true;
                    }
                }

                this.root = new RootProcessor();

                foreach(var source in sourceOperators)
                {
                    this.ApplyChildNodes(source.Value, this.root, root);
                    this.root.SetNextProcessor(source.Value);
                }

                foreach(var builder in storeBuilders)
                {
                    stateStores.Add(builder.Key, builder.Value.build() as IStateStore);
                }
            }
            else
                throw new Exception("Topology already built !");
        }

        private void ApplyChildNodes(IProcessor value, IProcessor previous, StreamGraphNode root)
        {
            StreamGraphNode r = null;
            while(r == null)
            {
                if ((root is ITableSourceNode && (((ITableSourceNode)root).SourceName.Equals(value.Name) ||
                        ((ITableSourceNode)root).NodeName.Equals(value.Name))) || value.Name.Equals(root.streamGraphNode))
                {
                    r = root;
                    break;
                }

                foreach (var i in root.ChildNodes)
                {
                    if ((i is ITableSourceNode && (((ITableSourceNode)i).SourceName.Equals(value.Name) ||
                        ((ITableSourceNode)i).NodeName.Equals(value.Name))) || value.Name.Equals(i.streamGraphNode))
                        r = i;
                }
            }

            if(r != null)
            {
                value.SetPreviousProcessor(previous);
                if(r is ITableSourceNode)
                {
                    var tableSourceProcessor = processorOperators.FirstOrDefault(kp => kp.Key.Equals((r as ITableSourceNode).NodeName)).Value;
                    if (tableSourceProcessor != null)
                    {
                        value.SetNextProcessor(tableSourceProcessor);
                        value = tableSourceProcessor;
                    }
                }

                IList<StreamGraphNode> list = r.ChildNodes;
                foreach (var n in list)
                {
                    if (n is StreamSinkNode)
                    {
                        var f = sinkOperators.FirstOrDefault(kp => kp.Key.Equals(n.streamGraphNode)).Value;
                        if (f != null)
                            value.SetNextProcessor(f);
                    }
                    else if (n is ProcessorGraphNode || n is TableProcessorNode)
                    {
                        var f = processorOperators.FirstOrDefault(kp => kp.Key.Equals(n.streamGraphNode)).Value;
                        if (f != null)
                        {
                            value.SetNextProcessor(f);
                            this.ApplyChildNodes(f, value, n);
                        }
                    }
                }
            }
        }

        private void ConnectProcessorAndStateStore(String processorName, String stateStoreName)
        {
            // TODO : 
            //if (globalStateBuilders.containsKey(stateStoreName))
            //{
            //    throw new TopologyException("Global StateStore " + stateStoreName +
            //            " can be used by a Processor without being specified; it should not be explicitly passed.");
            //}
            if (!storeBuilders.ContainsKey(stateStoreName))
            {
                throw new TopologyException("StateStore " + stateStoreName + " is not added yet.");
            }
            if (!processorOperators.ContainsKey(processorName))
            {
                throw new TopologyException("Processor " + processorName + " is not added yet.");
            }

            var processor = processorOperators[processorName];
            processor.StateStores.Add(stateStoreName);
        }

        #region Add Processors / State Store

        internal void AddSourceOperator<K,V>(string topic, string nameNode, ConsumedInternal<K, V> consumed)
        {
            if (!sourceOperators.ContainsKey(nameNode))
            {
                SourceProcessor<K, V> source = new SourceProcessor<K, V>(nameNode, topic, consumed.KeySerdes, consumed.ValueSerdes, consumed.TimestampExtractor);
                sourceOperators.Add(nameNode, source);
            }
            else
                throw new Exception("Source operator already exist !");
        }

        internal void AddSinkOperator<K, V>(ITopicNameExtractor<K,V> topicNameExtractor, string nameNode, Produced<K,V> produced)
        {
            if (!sinkOperators.ContainsKey(nameNode))
            {
                SinkProcessor<K, V> sink = new SinkProcessor<K, V>(nameNode, null, topicNameExtractor, produced.KeySerdes, produced.ValueSerdes);
                sinkOperators.Add(nameNode, sink);
            }
            else
                throw new Exception("Sink operator already exist !");
        }

        internal void AddProcessor<K, V>(string nameNode, IProcessorSupplier<K, V> processor)
        {
            if (!processorOperators.ContainsKey(nameNode))
            {
                var p = processor.Get();
                p.SetProcessorName(nameNode);
                processorOperators.Add(nameNode, p);
            }
            else
                throw new Exception("Processor operator already exist !");
        }

        internal  void AddStateStore<S>(StoreBuilder<S> storeBuilder, params String [] processorNames)
            where  S : IStateStore
        {
            this.AddStateStore<S>(storeBuilder, false, processorNames);
        }

        internal void AddStateStore<S>(StoreBuilder<S> storeBuilder, bool allowOverride, params String [] processorNames)
            where S : IStateStore
        {
            if (!allowOverride && storeBuilders.ContainsKey(storeBuilder.Name))
            {
                throw new TopologyException("StateStore " + storeBuilder.Name + " is already added.");
            }

            storeBuilders.Add(storeBuilder.Name, storeBuilder);

            if (processorNames != null)
            {
                foreach ( String processorName in processorNames)
                {
                    ConnectProcessorAndStateStore(processorName, storeBuilder.Name);
                }
            }
        }

        //private void buildProcessorNode(final Map<String, ProcessorNode> processorMap,
        //                        final Map<String, StateStore> stateStoreMap,
        //                        final ProcessorNodeFactory factory,
        //                        final ProcessorNode node)
        //{

        //    for (final String predecessor : factory.predecessors)
        //    {
        //        final ProcessorNode<?, ?> predecessorNode = processorMap.get(predecessor);
        //        predecessorNode.addChild(node);
        //    }
        //    for (final String stateStoreName : factory.stateStoreNames)
        //    {
        //        if (!stateStoreMap.containsKey(stateStoreName))
        //        {
        //            if (stateFactories.containsKey(stateStoreName))
        //            {
        //                final StateStoreFactory stateStoreFactory = stateFactories.get(stateStoreName);

        //                // remember the changelog topic if this state store is change-logging enabled
        //                if (stateStoreFactory.loggingEnabled() && !storeToChangelogTopic.containsKey(stateStoreName))
        //                {
        //                    final String changelogTopic = ProcessorStateManager.storeChangelogTopic(applicationId, stateStoreName);
        //                    storeToChangelogTopic.put(stateStoreName, changelogTopic);
        //                }
        //                stateStoreMap.put(stateStoreName, stateStoreFactory.build());
        //            }
        //            else
        //            {
        //                stateStoreMap.put(stateStoreName, globalStateStores.get(stateStoreName));
        //            }
        //        }
        //    }
        //}


        #endregion

        #region Describe

        internal ITopologyDescription Describe()
        {
            var topologyDes = new TopologyDescription();
            // TODO : 
            return topologyDes;
        }

        #endregion
    }
}