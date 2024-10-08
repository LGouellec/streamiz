using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table.Internal.Graph
{
    internal class KTableSuppress<K, S, V> : IKTableProcessorSupplier<K, V, V>
    {
        internal class KTableSuppressValueGetter : IKTableValueGetter<K, V>
        {
            private readonly IKTableValueGetter<K, V> _parentKTableValueGetter;
            private readonly string _storeName;

            public KTableSuppressValueGetter(IKTableValueGetterSupplier<K,V> parentKTableValueGetterSupplier, string storeName)
            {
                _parentKTableValueGetter = parentKTableValueGetterSupplier.Get();
                _storeName = storeName;
            }

            public void Init(ProcessorContext context)
            {
                _parentKTableValueGetter.Init(context);
                context.GetStateStore(_storeName);
            }

            public ValueAndTimestamp<V> Get(K key)
            {
                throw new System.NotImplementedException();
            }

            public void Close()
            {
                _parentKTableValueGetter.Close();
            }
        }
        
        private readonly Suppressed<K> _suppressed;
        private readonly string _storeName;
        private readonly KTable<K, S, V> _parent;

        public KTableSuppress(
            Suppressed<K> suppressed,
            string storeName,
            KTable<K, S, V> parentTable)
        {
            _suppressed = suppressed;
            _storeName = storeName;
            _parent = parentTable;
            // The suppress buffer requires seeing the old values, to support the prior value view.
            _parent.EnableSendingOldValues();
        }


        public void EnableSendingOldValues()
            => _parent.EnableSendingOldValues();

        public IProcessor<K, Change<V>> Get()
        {
            throw new System.NotImplementedException();
        }

        public IKTableValueGetterSupplier<K, V> View
        {
            get
            {
                var parentKTableValueGetterSupplier = _parent.ValueGetterSupplier;
                return new GenericKTableValueGetterSupplier<K, V>(
                    parentKTableValueGetterSupplier.StoreNames,
                    new KTableSuppressValueGetter(parentKTableValueGetterSupplier, _storeName));
            }
        }
    }
}