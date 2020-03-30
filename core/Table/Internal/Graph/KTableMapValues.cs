using kafka_stream_core.Processors;
using kafka_stream_core.State;
using kafka_stream_core.Stream;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal.Graph
{
    internal class KTableMapValues<K, V, VR> : IKTableProcessorSupplier<K, V, VR>
    {
        internal class KTableMapValuesValueGetter<K, V, VR> : IKTableValueGetter<K, VR>
        {
            private readonly IKTableValueGetter<K, V> ktablegetter;
            private readonly IValueMapperWithKey<K, V, VR> mapper;

            public KTableMapValuesValueGetter(IValueMapperWithKey<K, V, VR> mapper, IKTableValueGetter<K, V> getter)
            {
                this.ktablegetter = getter;
                this.mapper = mapper;
            }

            public void close() => ktablegetter.close();

            public ValueAndTimestamp<VR> get(K key) => computeValue(key, ktablegetter.get(key));

            public void init(ProcessorContext context) => ktablegetter.init(context);

            private ValueAndTimestamp<VR> computeValue(K key, ValueAndTimestamp<V> valueAndTimestamp)
            {
                VR newValue = default(VR);
                long timestamp = 0;

                if (valueAndTimestamp != null)
                {
                    newValue = mapper.apply(key, valueAndTimestamp.Value);
                    timestamp = valueAndTimestamp.Timestamp;
                }

                return ValueAndTimestamp<VR>.make(newValue, timestamp);
            }
        }

        private readonly KTableGetter<K, V> parentTable;
        private readonly IValueMapperWithKey<K, V, VR> mapper;
        private readonly string queryableName;
        private bool sendOldValues = false;

        public KTableMapValues(KTableGetter<K, V> parent, IValueMapperWithKey<K, V, VR> mapper, string queryableName)
        {
            this.parentTable = parent;
            this.mapper = mapper;
            this.queryableName = queryableName;
        }

        public IKTableValueGetterSupplier<K, VR> View
        {
            get
            {
                // if the KTable is materialized, use the materialized store to return getter value;
                // otherwise rely on the parent getter and apply filter on-the-fly
                if (this.queryableName != null)
                {
                    return new KTableMaterializedValueGetterSupplier<K, VR>(queryableName);
                }
                else
                {
                    var supplier = parentTable.ValueGetterSupplier;
                    return new GenericKTableValueGetterSupplier<K, VR>(
                        supplier.StoreNames,
                        new KTableMapValuesValueGetter<K, V, VR>(this.mapper, supplier.get()));
                }
            }
        }

        public void enableSendingOldValues()
        {
            parentTable.EnableSendingOldValues();
            sendOldValues = true;
        }

        public IProcessor<K, Change<V>> Get() => new KTableMapValuesProcessor<K, V, VR>(mapper, sendOldValues, queryableName);
    }
}
