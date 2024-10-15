using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Suppress;

namespace Streamiz.Kafka.Net.Table.Internal.Graph
{
    internal class KTableSuppress<K, S, V> : IKTableProcessorSupplier<K, V, V>
    {
        private class KTableSuppressValueGetter : IKTableValueGetter<K, V>
        {
            private readonly IKTableValueGetter<K, V> _parentKTableValueGetter;
            private readonly string _storeName;
            private ITimeOrderedKeyValueBuffer<K,V,Change<V>> buffer;

            public KTableSuppressValueGetter(IKTableValueGetterSupplier<K,V> parentKTableValueGetterSupplier, string storeName)
            {
                _parentKTableValueGetter = parentKTableValueGetterSupplier.Get();
                _storeName = storeName;
            }

            public void Init(ProcessorContext context)
            {
                _parentKTableValueGetter.Init(context);
                buffer = (ITimeOrderedKeyValueBuffer<K, V, Change<V>>)context.GetStateStore(_storeName);
            }

            public ValueAndTimestamp<V> Get(K key)
            {
                var container = buffer.PriorValueForBuffered(key);
                return container.IsDefined ? container.Value : _parentKTableValueGetter.Get(key);
            }

            public void Close()
            {
                _parentKTableValueGetter.Close();
            }
        }
        
        private readonly Suppressed<K, V> _suppressed;
        private readonly string _storeName;
        private readonly KTable<K, S, V> _parent;

        public KTableSuppress(
            Suppressed<K, V> suppressed,
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
            => new KTableSuppressProcessor<K, V>(
                _suppressed,
                _storeName);

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