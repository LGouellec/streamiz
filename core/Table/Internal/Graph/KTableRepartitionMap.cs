using kafka_stream_core.Processors;
using kafka_stream_core.State;
using kafka_stream_core.Stream;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal.Graph
{
    internal class KTableRepartitionMap<K, V, K1, V1> : IKTableProcessorSupplier<K, V, KeyValuePair<K1, V1>>
    {
        internal class KTableMapValueGetter : IKTableValueGetter<K, KeyValuePair<K1, V1>>
        {
            private IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;
            private IKTableValueGetter<K, V> parentTableGetter;
            private ProcessorContext context;

            public KTableMapValueGetter(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper, IKTableValueGetter<K, V> parentTableGetter)
            {
                this.mapper = mapper;
                this.parentTableGetter = parentTableGetter;
            }

            public void close() => parentTableGetter.close();

            public ValueAndTimestamp<KeyValuePair<K1, V1>> get(K key)
            {
                ValueAndTimestamp<V> valueAndTimestamp = parentTableGetter.get(key);
                var v = mapper.apply(key, valueAndTimestamp != null ? default : valueAndTimestamp.Value);
                return ValueAndTimestamp<KeyValuePair<K1, V1>>.make(v, valueAndTimestamp == null ? context.Timestamp : valueAndTimestamp.Timestamp);
            }

            public void init(ProcessorContext context)
            {
                this.context = context;
                parentTableGetter.init(context);
            }
        }

        private readonly KTableGetter<K, V> parentTable;
        private readonly IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;

        public KTableRepartitionMap(KTableGetter<K, V> parent, IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper)
        {
            this.parentTable = parent;
            this.mapper = mapper;
        }

        public IKTableValueGetterSupplier<K, KeyValuePair<K1, V1>> View
        {
            get
            {
                var supplier = parentTable.ValueGetterSupplier;
                return new GenericKTableValueGetterSupplier<K, KeyValuePair<K1, V1>>(
                    null,
                    new KTableMapValueGetter(this.mapper, supplier.get()));
            }
        }

        public void enableSendingOldValues()
        {
            throw new InvalidOperationException("KTableRepartitionMap should always require sending old values.");
        }

        public IProcessor<K, Change<V>> Get() => new KTableMapProcessor<K, V, K1, V1>(this.mapper);
    }
}
