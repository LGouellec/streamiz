using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using System;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class KTableKTableInnerJoinValueGetterSupplier<K, V1, V2, VR> :
        AbstractKTableKTableJoinValueGetterSupplier<K, V1, V2, VR>
    {
        private readonly IValueJoiner<V1, V2, VR> joiner;

        internal class KTableKTableInnerJoinValueGetter : IKTableValueGetter<K, VR>
        {
            private readonly IKTableValueGetter<K, V1> iKTableValueGetter1;
            private readonly IKTableValueGetter<K, V2> iKTableValueGetter2;
            private readonly IValueJoiner<V1, V2, VR> joiner;

            public KTableKTableInnerJoinValueGetter(IKTableValueGetter<K, V1> iKTableValueGetter1, IKTableValueGetter<K, V2> iKTableValueGetter2, IValueJoiner<V1, V2, VR> joiner) 
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

                if (valueAndTimestamp1 != null)
                {
                    ValueAndTimestamp<V2> valueAndTimestamp2 = iKTableValueGetter2.Get(key);

                    if (valueAndTimestamp2 != null)
                    {
                        return ValueAndTimestamp<VR>.Make(
                            joiner.Apply(valueAndTimestamp1.Value, valueAndTimestamp2.Value),
                            Math.Max(valueAndTimestamp1.Timestamp, valueAndTimestamp2.Timestamp));
                    }
                    else
                    {
                        return null;
                    }
                }
                else
                {
                    return null;
                }
            }

            public void Init(ProcessorContext context)
            {
                iKTableValueGetter1.Init(context);
                iKTableValueGetter2.Init(context);
            }
        }

        public KTableKTableInnerJoinValueGetterSupplier(IKTableValueGetterSupplier<K, V1> valueGetterSupplier1, IKTableValueGetterSupplier<K, V2> valueGetterSupplier2, IValueJoiner<V1, V2, VR> valueJoiner)
            : base(valueGetterSupplier1, valueGetterSupplier2)
        {
            joiner = valueJoiner;
        }

        public override IKTableValueGetter<K, VR> Get()
            => new KTableKTableInnerJoinValueGetter(getter1.Get(), getter2.Get(), joiner);
    }
}
