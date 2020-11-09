using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableKTableJoinMergeProcessor<K, V> : AbstractKTableProcessor<K, V, K, V>
    {
        public KTableKTableJoinMergeProcessor(string queryableStoreName, bool sendOldValues, bool throwExceptionStateNull = false)
            : base(queryableStoreName, sendOldValues, throwExceptionStateNull)
        {
        }

        public override void Process(K key, Change<V> value)
        {
            if (!string.IsNullOrEmpty(queryableStoreName))
            {
                store.Put(key, ValueAndTimestamp<V>.Make(value.NewValue, Context.Timestamp));
                tupleForwarder.MaybeForward(key, value.NewValue, sendOldValues ? value.OldValue : default);
            }
            else
            {
                if (sendOldValues)
                {
                    Forward(key, value);
                }
                else
                {
                    Forward(key, new Change<V>(value.NewValue, default));
                }
            }
        }
    }
}
