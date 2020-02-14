using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream.Internal.Graph.Nodes
{
    internal class RootNode : StreamGraphNode
    {
        public RootNode() : base("ROOT-NODE")
        {
        }

        public override void writeToTopology(InternalTopologyBuilder builder)
        {
        }
    }
}
