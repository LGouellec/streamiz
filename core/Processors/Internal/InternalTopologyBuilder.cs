using kafka_stream_core.Errors;
using kafka_stream_core.State;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace kafka_stream_core.Processors.Internal
{
    internal class InternalTopologyBuilder
    {
        private IDictionary<string, IProcessor> sourceOperators = new Dictionary<string, IProcessor>();
        private IDictionary<string, IProcessor> sinkOperators = new Dictionary<string, IProcessor>();
        private IDictionary<string, IProcessor> processorOperators = new Dictionary<string, IProcessor>();
        private IDictionary<string, StateStore> stateStores = new Dictionary<string, StateStore>();

        private IDictionary<string, StoreBuilder> storeBuilders = new Dictionary<string, StoreBuilder>();

        private IProcessor root = null;

        internal InternalTopologyBuilder()
        {
        }

        public ProcessorTopology buildTopology()
        {
            var topology = new ProcessorTopology(root, sourceOperators, sinkOperators, processorOperators, stateStores);
            return topology;
        }

        internal IEnumerable<string> GetSourceTopics() => sourceOperators.Values.Select(o => (o as ISourceProcessor).TopicName);

        internal void buildAndOptimizeTopology(RootNode root, IList<StreamGraphNode> nodes)
        {
            if (this.root == null)
            {
                foreach (var node in nodes)
                {
                    if (node.allParentsWrittenToTopology() && !node.HasWrittenToTopology)
                    {
                        node.writeToTopology(this);
                        node.HasWrittenToTopology = true;
                    }
                }

                this.root = new RootProcessor();

                foreach(var source in sourceOperators)
                {
                    this.applyChildNodes(source.Value, this.root, root);
                    this.root.SetNextProcessor(source.Value);
                }

                foreach(var builder in storeBuilders)
                {
                    stateStores.Add(builder.Key, builder.Value.build() as StateStore);
                }
            }
            else
                throw new Exception("Topology already built !");
        }

        private void applyChildNodes(IProcessor value, IProcessor previous, StreamGraphNode root)
        {
            StreamGraphNode r = null;
            while(r == null)
            {
                if (value.Name.Equals(root.streamGraphNode))
                {
                    r = root;
                    break;
                }

                foreach (var i in root.ChildNodes)
                    if (value.Name.Equals(i.streamGraphNode))
                        r = i;
            }

            if(r != null)
            {
                value.SetPreviousProcessor(previous);
                IList<StreamGraphNode> list = r.ChildNodes;
                foreach (var n in list)
                {
                    if (n is StreamSinkNode)
                    {
                        var f = sinkOperators.FirstOrDefault(kp => kp.Key.Equals(n.streamGraphNode)).Value;
                        if (f != null)
                            value.SetNextProcessor(f);
                    }
                    else if (n is ProcessorGraphNode)
                    {
                        var f = processorOperators.FirstOrDefault(kp => kp.Key.Equals(n.streamGraphNode)).Value;
                        if (f != null)
                        {
                            value.SetNextProcessor(f);
                            this.applyChildNodes(f, value, n);
                        }
                    }
                }
            }
        }

        private void connectProcessorAndStateStore(String processorName, String stateStoreName)
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

        internal void addSourceOperator<K,V>(string topic, string nameNode, Consumed<K,V> consumed)
        {
            if (!sourceOperators.ContainsKey(nameNode))
            {
                SourceProcessor<K, V> source = new SourceProcessor<K, V>(nameNode, topic, consumed.KeySerdes, consumed.ValueSerdes, consumed.TimestampExtractor, consumed.AutoOffsetReset);
                sourceOperators.Add(nameNode, source);
            }
            else
                throw new Exception("Source operator already exist !");
        }

        internal void addSinkOperator<K, V>(TopicNameExtractor<K,V> topicNameExtractor, string nameNode, Produced<K,V> produced)
        {
            if (!sinkOperators.ContainsKey(nameNode))
            {
                SinkProcessor<K, V> sink = new SinkProcessor<K, V>(nameNode, null, topicNameExtractor, produced.KeySerdes, produced.ValueSerdes);
                sinkOperators.Add(nameNode, sink);
            }
            else
                throw new Exception("Sink operator already exist !");
        }

        internal void addProcessor<K, V>(string nameNode, IProcessorSupplier<K, V> processor)
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

        internal  void addStateStore<S>(StoreBuilder<S> storeBuilder, params String [] processorNames)
            where  S : StateStore
        {
            this.addStateStore<S>(storeBuilder, false, processorNames);
        }

        internal void addStateStore<S>(StoreBuilder<S> storeBuilder, bool allowOverride, params String [] processorNames)
            where S : StateStore
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
                    connectProcessorAndStateStore(processorName, storeBuilder.Name);
                }
            }
        }

        #endregion
    }
}