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

            public void Close() => parentTableGetter.Close();

            public ValueAndTimestamp<KeyValuePair<K1, V1>> Get(K key)
            {
                ValueAndTimestamp<V> valueAndTimestamp = parentTableGetter.Get(key);
                var v = mapper.Apply(key, valueAndTimestamp != null ? default : valueAndTimestamp.Value);
                return ValueAndTimestamp<KeyValuePair<K1, V1>>.Make(v, valueAndTimestamp == null ? context.Timestamp : valueAndTimestamp.Timestamp);
            }

            public void Init(ProcessorContext context)
            {
                this.context = context;
                parentTableGetter.Init(context);
            }
        }

        private readonly IKTableGetter<K, V> parentTable;
        private readonly IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;

        public KTableRepartitionMap(IKTableGetter<K, V> parent, IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper)
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
                    new KTableMapValueGetter(this.mapper, supplier.Get()));
            }
        }

        public void EnableSendingOldValues()
        {
            throw new InvalidOperationException("KTableRepartitionMap should always require sending old values.");
        }

        public IProcessor<K, Change<V>> Get() => new KTableMapProcessor<K, V, K1, V1>(this.mapper);
    }
}
