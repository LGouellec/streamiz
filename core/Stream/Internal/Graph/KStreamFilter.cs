using Kafka.Streams.Net.Processors;
using System;

namespace Kafka.Streams.Net.Stream.Internal.Graph.Nodes
{
    internal class KStreamFilter<K, V> : IProcessorSupplier<K, V>
    {
        public Func<K, V, bool> Predicate { get; }
        public bool Not { get; }


        public KStreamFilter(Func<K, V, bool> predicate, bool not = false)
        {
            Predicate = predicate;
            Not = not;
        }

        public IProcessor<K, V> Get() => new KStreamFilterProcessor<K, V>(this.Predicate, this.Not);
    }
}
