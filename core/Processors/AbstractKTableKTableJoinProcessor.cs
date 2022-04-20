using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractKTableKTableJoinProcessor<K, V1, V2, VR> : AbstractProcessor<K, Change<V1>>
    {
        protected readonly IKTableValueGetter<K, V2> valueGetter;
        protected readonly IValueJoiner<V1, V2, VR> joiner;
        protected readonly bool sendOldValues;
        protected readonly string joinResultTopic;

        public AbstractKTableKTableJoinProcessor(IKTableValueGetter<K, V2> valueGetter, IValueJoiner<V1, V2, VR> joiner, bool sendOldValues, string joinResultTopic = null)
        {
            this.valueGetter = valueGetter;
            this.joiner = joiner;
            this.sendOldValues = sendOldValues;
            this.joinResultTopic = joinResultTopic;
        }

        public override void Close()
        {
            base.Close();
            valueGetter.Close();
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            valueGetter.Init(context);
        }
    }
}