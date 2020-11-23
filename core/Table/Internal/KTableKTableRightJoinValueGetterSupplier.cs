using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using System;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class KTableKTableRightJoinValueGetterSupplier<K, V1, V2, VR> :
        AbstractKTableKTableJoinValueGetterSupplier<K, V1, V2, VR>
    {
        private readonly IValueJoiner<V1, V2, VR> joiner;

        internal class KTableKTableRightJoinValueGetter : IKTableValueGetter<K, VR>
        {
            private readonly IKTableValueGetter<K, V1> iKTableValueGetter1;
            private readonly IKTableValueGetter<K, V2> iKTableValueGetter2;
            private readonly IValueJoiner<V1, V2, VR> joiner;

            public KTableKTableRightJoinValueGetter(IKTableValueGetter<K, V1> iKTableValueGetter1, IKTableValueGetter<K, V2> iKTableValueGetter2, IValueJoiner<V1, V2, VR> joiner)
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
                ValueAndTimestamp<V2> valueAndTimestamp2 = iKTableValueGetter2.Get(key);

                if (valueAndTimestamp2 != null)
                {
                    ValueAndTimestamp<V1> valueAndTimestamp1 = iKTableValueGetter1.Get(key);
                    long resultTimestamp;
                    if (valueAndTimestamp1 != null)
                    {
                        resultTimestamp = Math.Max(valueAndTimestamp2.Timestamp, valueAndTimestamp1.Timestamp);
                    }
                    else
                    {
                        resultTimestamp = valueAndTimestamp2.Timestamp;
                    }

                    return ValueAndTimestamp<VR>.Make(
                            joiner.Apply(valueAndTimestamp1 != null ? valueAndTimestamp1.Value : default, valueAndTimestamp2.Value),
                            resultTimestamp);
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


        public KTableKTableRightJoinValueGetterSupplier(IKTableValueGetterSupplier<K, V1> getter1, IKTableValueGetterSupplier<K, V2> getter2, IValueJoiner<V1, V2, VR> valueJoiner)
            : base(getter1, getter2)
        {
            joiner = valueJoiner;
        }

        public override IKTableValueGetter<K, VR> Get()
            => new KTableKTableRightJoinValueGetter(getter1.Get(), getter2.Get(), joiner);
    }
}
