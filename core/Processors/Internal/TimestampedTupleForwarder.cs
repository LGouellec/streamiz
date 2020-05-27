using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    // TODO REFACTOR
    internal class TimestampedTupleForwarder<K, V>
    {
        private readonly IProcessor<K, V> processor;
        private readonly IProcessor<K, Change<V>> changeProcessor;
        private readonly bool sendOldValues;
        private readonly bool cachingEnabled;

        public TimestampedTupleForwarder(IProcessor<K, V> processor, bool sendOldValues)
        {
            this.processor = processor;
            this.changeProcessor = null;
            this.sendOldValues = sendOldValues;
            cachingEnabled = false;
        }

        public TimestampedTupleForwarder(IProcessor<K, Change<V>> changeProcessor, bool sendOldValues)
        {
            this.changeProcessor = changeProcessor;
            this.processor = null;
            this.sendOldValues = sendOldValues;
            cachingEnabled = false;
        }

        public void MaybeForward(K key, V newValue, V oldValue)
        {
            if (!cachingEnabled)
            {
                if (processor != null)
                    processor.Forward(key, new Change<V>(sendOldValues ? oldValue : default, newValue));
                else if (changeProcessor != null)
                    changeProcessor.Forward(key, new Change<V>(sendOldValues ? oldValue : default, newValue));
            }
        }

        public void MaybeForward<VR>(K key, VR newValue, VR oldValue)
        {
            if (!cachingEnabled)
            {
                if (processor != null)
                    processor.Forward(key, new Change<VR>(sendOldValues ? oldValue : default, newValue));
                else if (changeProcessor != null)
                    changeProcessor.Forward(key, new Change<VR>(sendOldValues ? oldValue : default, newValue));
            }
        }

        public void MaybeForward(K key,
                                  V newValue,
                                  V oldValue,
                                  long timestamp)
        {
            if (processor != null)
                processor.Forward(key, new Change<V>(sendOldValues ? oldValue : default, newValue));
            else if (changeProcessor != null)
                changeProcessor.Forward(key, new Change<V>(sendOldValues ? oldValue : default, newValue));
        }

        public void MaybeForward<VR>(K key,
                          VR newValue,
                          VR oldValue,
                          long timestamp)
        {
            if (!cachingEnabled)
            {
                if (processor != null)
                    processor.Forward(key, new Change<VR>(sendOldValues ? oldValue : default, newValue));
                else if (changeProcessor != null)
                    changeProcessor.Forward(key, new Change<VR>(sendOldValues ? oldValue : default, newValue));
            }
        }
    }
}
