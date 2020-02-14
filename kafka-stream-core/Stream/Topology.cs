using System;
using System.Collections.Generic;
using kafka_stream_core.Stream.Internal.Graph.Nodes;

namespace kafka_stream_core.Stream
{
    public class Topology
    {
        internal InternalTopologyBuilder Builder { get; } = new InternalTopologyBuilder();

        internal RootNode RootNode { get; private set; }
        internal IList<StreamGraphNode> Nodes { get; private set; }


        internal Topology()
        {
        }

        internal void SetNodes(RootNode root, IList<StreamGraphNode> nodes)
        {
            this.RootNode = root;
            this.Nodes = nodes;
        }
    }
}
