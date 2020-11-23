using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;
using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableKTableLeftJoin<K, VR, V1, V2> : AbstractKTableKTableJoin<K, VR, V1, V2>
    {
        public KTableKTableLeftJoin(IKTableGetter<K, V1> table1, IKTableGetter<K, V2> table2, IValueJoiner<V1, V2, VR> valueJoiner)
            : base(table1, table2, valueJoiner)
        {
        }

        public override IKTableValueGetterSupplier<K, VR> View
            => new KTableKTableLeftJoinValueGetterSupplier<K, V1, V2, VR>(table1.ValueGetterSupplier, table2.ValueGetterSupplier, valueJoiner);

        public override IProcessor<K, Change<V1>> Get()
            => new KTableKTableLeftJoinProcessor<K, V1, V2, VR>(table2.ValueGetterSupplier.Get(), valueJoiner, sendOldValues);
    }
}
