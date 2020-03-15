using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream.Internal.Graph
{
    internal class KStreamBranch<K, V> : IProcessorSupplier<K, V>
    {
        public Func<K, V, bool>[] Predicates { get; }
        public String[] ChildNodes { get; }

        public KStreamBranch(Func<K, V, bool>[] predicates,
                      String[] childNodes)
        {
            this.Predicates = predicates;
            this.ChildNodes = childNodes;
        }

        public IProcessor<K, V> Get() => new KStreamBranchProcessor<K, V>(this.Predicates, this.ChildNodes);
    }
}
