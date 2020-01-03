using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Nodes
{
    internal abstract class StreamGraphNode
    {
        private readonly Guid guid = Guid.NewGuid();
        internal readonly string streamGraphNode;
        private readonly IList<StreamGraphNode> childNode = new List<StreamGraphNode>();

        internal StreamGraphNode(string streamGraphNode)
        {
            this.streamGraphNode = streamGraphNode;
        }

        public bool IsEmpty => childNode.Count == 0;
        public IList<StreamGraphNode> Nodes => childNode;

        public void appendChild(StreamGraphNode node)
        {
            childNode.Add(node);
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
