using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal abstract class StreamGraphNode
    {
        private readonly Guid guid = Guid.NewGuid();
        internal readonly string streamGraphNode;

        public IList<StreamGraphNode> ChildNodes { get; } = new List<StreamGraphNode>();

        public IList<StreamGraphNode> ParentNodes { get; } = new List<StreamGraphNode>();

        public bool KeyChangingOperation { get; internal set; }
        public bool ValueChangingOperation { get; internal set; }
        public bool MergeNode { get; internal set; }
        public int BuildPriority { get; internal set; }
        public bool HasWrittenToTopology { get; internal set; } = false;


        protected StreamGraphNode(string streamGraphNode)
        {
            this.streamGraphNode = streamGraphNode;
        }

        public bool IsEmpty => ChildNodes.Count == 0;
        internal string[] ParentNodeNames() => ParentNodes.Select(p => p.streamGraphNode).ToArray();
        public bool AllParentsWrittenToTopology
        {
            get
            {
                foreach (var parentNode in ParentNodes)
                    if (!parentNode.HasWrittenToTopology)
                        return false;
                return true;
            }
        }

        public void AppendChild(StreamGraphNode node)
        {
            if (!ChildNodes.Contains(node))
            {
                ChildNodes.Add(node);
                node.ParentNodes.Add(this);
            }
        }

        public bool RemoveChild(StreamGraphNode node)
        {
            return ChildNodes.Remove(node) && node.ParentNodes.Remove(this);
        }

        public void ClearChildren()
        {
            foreach (var childNode in ChildNodes)
                childNode.ParentNodes.Remove(this);
            ChildNodes.Clear();
        }


        public abstract void WriteToTopology(InternalTopologyBuilder builder);

        public override bool Equals(object obj)
        {
            return obj is StreamGraphNode && (obj as StreamGraphNode).guid.Equals(this.guid);
        }

        public override int GetHashCode()
        {
            return guid.GetHashCode();
        }

        public override string ToString()
        {
            var parentNames = ParentNodes.Select(n => n.streamGraphNode);
            return "StreamsGraphNode{" +
                   "nodeName='" + this.streamGraphNode + '\'' +
                   ", buildPriority=" + BuildPriority +
                   ", hasWrittenToTopology=" + HasWrittenToTopology +
                   ", keyChangingOperation=" + KeyChangingOperation +
                   ", valueChangingOperation=" + ValueChangingOperation +
                   ", mergeNode=" + MergeNode +
                   ", parentNodes=" + string.Join(",", parentNames) + '}';
        }
    }
}
