using System;
using kafka_stream_core.Processors;

namespace kafka_stream_core.Table.Internal.Graph
{
    internal class KTableFilter<K, V> : IKTableProcessorSupplier<K, V, V>
    {
        private KTableImpl<K, V> kTableImpl;
        private Func<K, V, bool> predicate;
        private bool filterNot;
        private string queryableStoreName;

        public KTableFilter(KTableImpl<K, V> kTableImpl, Func<K, V, bool> predicate, bool filterNot, string queryableStoreName)
        {
            this.kTableImpl = kTableImpl;
            this.predicate = predicate;
            this.filterNot = filterNot;
            this.queryableStoreName = queryableStoreName;
        }

        public IKTableValueGetterSupplier<K, V> View => throw new NotImplementedException();

        public void enableSendingOldValues()
        {
            throw new NotImplementedException();
        }

        public IProcessor<K, Change<V>> Get()
        {
            throw new NotImplementedException();
        }
    }
}
