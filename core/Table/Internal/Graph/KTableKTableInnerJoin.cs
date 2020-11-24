using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Table.Internal.Graph
{
    internal class KTableKTableInnerJoin<K, VR, V1, V2> : AbstractKTableKTableJoin<K, VR, V1, V2>
    {
        public KTableKTableInnerJoin(IKTableGetter<K, V1> table1, IKTableGetter<K, V2> table2, IValueJoiner<V1, V2, VR> valueJoiner)
            : base(table1, table2, valueJoiner)
        { }

        public override IKTableValueGetterSupplier<K, VR> View
            => new KTableKTableInnerJoinValueGetterSupplier<K, V1, V2, VR>(table1.ValueGetterSupplier, table2.ValueGetterSupplier, valueJoiner);

        public override IProcessor<K, Change<V1>> Get()
            => new KTableKTableJoinProcessor<K, V1, V2, VR>(table2.ValueGetterSupplier.Get(), valueJoiner, sendOldValues);
    }
}
