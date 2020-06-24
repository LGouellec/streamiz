using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    // TODO REFACTOR
    internal class TimestampedTupleForwarder<K, V>
    {
        private readonly IProcessor processor;
        private readonly bool sendOldValues;
        private readonly bool cachingEnabled;

        public TimestampedTupleForwarder(IProcessor processor, bool sendOldValues)
        {
            this.processor = processor;
            this.sendOldValues = sendOldValues;
            cachingEnabled = false;
        }

        public void MaybeForward(K key, V newValue, V oldValue)
        {
            if (!cachingEnabled)
                processor?.Forward(key, new Change<V>(sendOldValues ? oldValue : default, newValue));
        }

        public void MaybeForward<VR>(K key, VR newValue, VR oldValue)
        {
            if (!cachingEnabled)
                processor?.Forward(key, new Change<VR>(sendOldValues ? oldValue : default, newValue));
        }

        public void MaybeForward(K key, V newValue, V oldValue, long timestamp)
        {
            processor?.Forward(key, new Change<V>(sendOldValues ? oldValue : default, newValue), timestamp);
        }

        public void MaybeForward<VR>(K key, VR newValue, VR oldValue, long timestamp)
        {
            if (!cachingEnabled)
                processor?.Forward(key, new Change<VR>(sendOldValues ? oldValue : default, newValue), timestamp);
        }
    }
}