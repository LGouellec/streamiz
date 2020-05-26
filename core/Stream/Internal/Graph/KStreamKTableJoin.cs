using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamKTableJoin<K, R, V1, V2> : IProcessorSupplier<K, V1>
        where R : class
        where V2 : class
    {
        public class KeyValueMapper : IKeyValueMapper<K, V1, K>
        {
            public K Apply(K key, V1 value) => key;
        }

        private readonly IKTableValueGetterSupplier<K, V2> valueGetter;
        private readonly IValueJoiner<V1, V2, R> valueJoiner;
        private readonly bool leftJoin;

        public KStreamKTableJoin(
            IKTableValueGetterSupplier<K, V2> valueGetter,
            IValueJoiner<V1, V2, R> valueJoiner,
            bool leftJoin)
        {
            this.valueGetter = valueGetter;
            this.valueJoiner = valueJoiner;
            this.leftJoin = leftJoin;
        }

        public IProcessor<K, V1> Get()
            => new KStreamKTableJoinProcessor<K, K, V1, V2, R>(valueGetter.Get(), new KeyValueMapper(), valueJoiner, leftJoin);
    }
}
