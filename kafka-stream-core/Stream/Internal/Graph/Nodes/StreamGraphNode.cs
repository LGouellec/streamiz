using kafka_stream_core.Processors.Internal;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Stream.Internal.Graph.Nodes
{
    internal abstract class StreamGraphNode
    {
        private readonly Guid guid = Guid.NewGuid();
        internal readonly string streamGraphNode;
        private readonly IList<StreamGraphNode> childNode = new List<StreamGraphNode>();
        private readonly IList<StreamGraphNode> parentNode = new List<StreamGraphNode>();

        internal StreamGraphNode(string streamGraphNode)
        {
            this.streamGraphNode = streamGraphNode;
        }

        public bool IsEmpty => childNode.Count == 0;
        public IList<StreamGraphNode> Nodes => childNode;

        public void appendChild(StreamGraphNode node)
        {
            if(!childNode.Contains(node))
                childNode.Add(node);
        }

        public void appendParent(StreamGraphNode node)
        {
            if(!parentNode.Contains(node))
                parentNode.Add(node);
        }

        public abstract void writeToTopology(InternalTopologyBuilder builder);

        public override bool Equals(object obj)
        {
            return obj is StreamGraphNode && (obj as StreamGraphNode).guid.Equals(this.guid);
        }

        public override int GetHashCode()
        {
            return guid.GetHashCode();
        }
    }
}
