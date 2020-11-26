using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using System;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class KTableKTableOuterJoinValueGetterSupplier<K, V1, V2, VR> :
        AbstractKTableKTableJoinValueGetterSupplier<K, V1, V2, VR>
    {
        private readonly IValueJoiner<V1, V2, VR> joiner;

        internal class KTableKTableOuterJoinValueGetter : IKTableValueGetter<K, VR>
        {
            private readonly IKTableValueGetter<K, V1> iKTableValueGetter1;
            private readonly IKTableValueGetter<K, V2> iKTableValueGetter2;
            private readonly IValueJoiner<V1, V2, VR> joiner;

            public KTableKTableOuterJoinValueGetter(IKTableValueGetter<K, V1> iKTableValueGetter1, IKTableValueGetter<K, V2> iKTableValueGetter2, IValueJoiner<V1, V2, VR> joiner)
            {
                this.iKTableValueGetter1 = iKTableValueGetter1;
                this.iKTableValueGetter2 = iKTableValueGetter2;
                this.joiner = joiner;
            }

            public void Close()
            {
                iKTableValueGetter1.Close();
                iKTableValueGetter2.Close();
            }

            public ValueAndTimestamp<VR> Get(K key)
            {
                ValueAndTimestamp<V1> valueAndTimestamp1 = iKTableValueGetter1.Get(key);
                ValueAndTimestamp<V2> valueAndTimestamp2 = iKTableValueGetter2.Get(key);

                VR newValue = default;
                V1 value1 = valueAndTimestamp1 == null ? default : valueAndTimestamp1.Value;
                long ts1 = valueAndTimestamp1 == null ? -1 : valueAndTimestamp1.Timestamp;
                V2 value2 = valueAndTimestamp2 == null ? default : valueAndTimestamp2.Value;
                long ts2 = valueAndTimestamp2 == null ? -1 : valueAndTimestamp2.Timestamp;

                if (value1 != null || value2 != null)
                {
                    newValue = joiner.Apply(value1, value2);
                }

                return ValueAndTimestamp<VR>.Make(newValue, Math.Max(ts1, ts2));
            }

            public void Init(ProcessorContext context)
            {
                iKTableValueGetter1.Init(context);
                iKTableValueGetter2.Init(context);
            }
        }

        public KTableKTableOuterJoinValueGetterSupplier(IKTableValueGetterSupplier<K, V1> getter1, IKTableValueGetterSupplier<K, V2> getter2, IValueJoiner<V1, V2, VR> valueJoiner)
            : base(getter1, getter2)
        {
            joiner = valueJoiner;
        }

        public override IKTableValueGetter<K, VR> Get()
            => new KTableKTableOuterJoinValueGetter(getter1.Get(), getter2.Get(), joiner);
    }
}
