using kafka_stream_core.Nodes.Parameters;
using kafka_stream_core.Processors;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace kafka_stream_core
{
    internal class InternalTopologyBuilder
    {
        private IDictionary<string, IProcessor> sourceOperators = new Dictionary<string, IProcessor>();
        private IDictionary<string, IProcessor> sinkOperators = new Dictionary<string, IProcessor>();
        private IDictionary<string, IProcessor> processorOperators = new Dictionary<string, IProcessor>();

        private IProcessor root = null;

        internal InternalTopologyBuilder()
        {
        }

        internal void buildAndOptimizeTopology(RootNode root, IList<StreamGraphNode> nodes)
        {
            if (this.root == null)
            {
                foreach (var node in nodes)
                    node.writeToTopology(this);

                this.root = new RootProcessor();

                foreach(var source in sourceOperators)
                {
                    this.applyChildNodes(source.Value, this.root, root);
                    this.root.SetNextProcessor(source.Value);
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

                foreach (var i in root.Nodes)
                    if (value.Name.Equals(i.streamGraphNode))
                        r = i;
            }

            if(r != null)
            {
                value.SetPreviousProcessor(previous);
                IList<StreamGraphNode> list = r.Nodes;
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

        internal void addSourceOperator<K,V>(string topic, string nameNode, Consumed<K,V> consumed)
        {
            if (!sourceOperators.ContainsKey(nameNode))
            {
                SourceProcessor<K, V> source = new SourceProcessor<K, V>(nameNode, topic, consumed.KeySerdes, consumed.ValueSerdes);
                sourceOperators.Add(nameNode, source);
            }
            else
                throw new Exception("Source operator already exist !");
        }

        internal void addSinkOperator<K, V>(string topic, string nameNode, Produced<K,V> produced)
        {
            if (!sinkOperators.ContainsKey(nameNode))
            {
                SinkProcessor<K, V> sink = new SinkProcessor<K, V>(nameNode, null, topic, produced.KeySerdes, produced.ValueSerdes);
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
                processorOperators.Add(nameNode, p);
            }
            else
                throw new Exception("Processor operator already exist !");
        }
    }
}
