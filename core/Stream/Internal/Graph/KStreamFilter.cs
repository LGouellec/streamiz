using Streamiz.Kafka.Net.Processors;
using System;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamFilter<K, V> : IProcessorSupplier<K, V>
    {
        public Func<K, V,IRecordContext, bool> Predicate { get; }
        public bool Not { get; }


        public KStreamFilter(Func<K, V,IRecordContext, bool> predicate, bool not = false)
        {
            Predicate = predicate;
            Not = not;
        }

        public IProcessor<K, V> Get() => new KStreamFilterProcessor<K, V>(this.Predicate, this.Not);
    }
}
