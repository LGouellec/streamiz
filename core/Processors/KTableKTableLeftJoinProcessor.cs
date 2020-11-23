using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;
using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableKTableLeftJoinProcessor<K, V1, V2, VR> : AbstractProcessor<K, Change<V1>>
    {
        private readonly IKTableValueGetter<K, V2> valueGetter;
        private readonly IValueJoiner<V1, V2, VR> joiner;
        private readonly bool sendOldValues;

        public KTableKTableLeftJoinProcessor(IKTableValueGetter<K, V2> iKTableValueGetter, IValueJoiner<V1, V2, VR> valueJoiner, bool sendOldValues)
        {
            valueGetter = iKTableValueGetter;
            joiner = valueJoiner;
            this.sendOldValues = sendOldValues;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            valueGetter.Init(context);
        }

        public override void Process(K key, Change<V1> value)
        {
            if (key == null)
            {
                log.Warn($"{logPrefix}Skipping record due to null key. change=[{value}] topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}]");
                return;
            }

            VR newValue = default;
            VR oldValue = default;
            var valueAndTsRight = valueGetter.Get(key);
            long timestampRight, resultTimestamp;

            if (valueAndTsRight == null)
            {
                if (value.NewValue == null && value.OldValue == null)
                {
                    return;
                }

                timestampRight = -1;
            }
            else
            {
                timestampRight = valueAndTsRight.Timestamp;
            }

            resultTimestamp = Math.Max(Context.Timestamp, timestampRight);

            if (value.NewValue != null)
            {
                newValue = joiner.Apply(value.NewValue, valueAndTsRight.Value);
            }

            if (sendOldValues && value.OldValue != null)
            {
                oldValue = joiner.Apply(value.OldValue, valueAndTsRight.Value);
            }

            Forward(key, new Change<VR>(oldValue, newValue), resultTimestamp);
        }

        public override void Close()
        {
            base.Close();
            valueGetter.Close();
        }
    }
}
