using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using kafka_stream_core.Nodes;
using kafka_stream_core.Nodes.Parameters;
using kafka_stream_core.Operators;

namespace kafka_stream_core
{
    internal class InternalTopologyBuilder
    {
        private IDictionary<string, IOperator> sourceOperators = new Dictionary<string, IOperator>();
        private IDictionary<string, IOperator> sinkOperators = new Dictionary<string, IOperator>();
        private IDictionary<string, IOperator> processorOperators = new Dictionary<string, IOperator>();

        private IOperator rootOperator = null;

        internal InternalTopologyBuilder()
        {
        }

        internal void buildAndOptimizeTopology(RootNode root, IList<StreamGraphNode> nodes)
        {
            if (rootOperator == null)
            {
                foreach (var node in nodes)
                    node.writeToTopology(this);

                rootOperator = new RootOperator();

                foreach(var source in sourceOperators)
                {
                    this.applyChildNodes(source.Value, rootOperator, root);
                    rootOperator.SetNextOperator(source.Value);
                }
            }
            else
                throw new Exception("Topology already built !");
        }

        private void applyChildNodes(IOperator value, IOperator previous, StreamGraphNode root)
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
                value.SetPreviousOperator(previous);
                IList<StreamGraphNode> list = r.Nodes;
                foreach (var n in list)
                {
                    if (n is StreamSinkNode)
                    {
                        var f = sinkOperators.FirstOrDefault(kp => kp.Key.Equals(n.streamGraphNode)).Value;
                        if (f != null)
                            value.SetNextOperator(f);
                    }
                    else if (n is ProcessorGraphNode)
                    {
                        var f = processorOperators.FirstOrDefault(kp => kp.Key.Equals(n.streamGraphNode)).Value;
                        if (f != null)
                        {
                            value.SetNextOperator(f);
                            this.applyChildNodes(f, value, n);
                        }
                    }
                }
            }
        }

        internal void setSourceOperator<K,V>(string topic, string nameNode, Consumed<K,V> consumed)
        {
            if (!sourceOperators.ContainsKey(nameNode))
            {
                SourceOperator<K, V> source = new SourceOperator<K, V>(nameNode, topic, consumed.KeySerdes, consumed.ValueSerdes);
                sourceOperators.Add(nameNode, source);
            }
            else
                throw new Exception("Source operator already exist !");
        }

        internal void setSinkOperator<K, V>(string topic, string nameNode, Produced<K,V> produced)
        {
            if (!sinkOperators.ContainsKey(nameNode))
            {
                SinkOperator<K, V> sink = new SinkOperator<K, V>(nameNode, null, topic, produced.KeySerdes, produced.ValueSerdes);
                sinkOperators.Add(nameNode, sink);
            }
            else
                throw new Exception("Sink operator already exist !");
        }

        internal void setFilterOperator<K,V>(string nameNode, Func<K, V, bool> predicate)
        {
            if (!processorOperators.ContainsKey(nameNode))
            {
                FilterOperator<K, V> filter = new FilterOperator<K, V>(null, nameNode, predicate);
                processorOperators.Add(nameNode, filter);
            }
            else
                throw new Exception("Filter operator already exist !");
        }
        
        internal Topology build()
        {
            return new Topology(rootOperator);
        }
    }
}
