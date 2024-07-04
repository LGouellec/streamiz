using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class TimestampedTupleForwarder<K, V>
    {
        private readonly IProcessor processor;
        private readonly bool sendOldValues;
        private readonly bool cachingEnabled;

        public TimestampedTupleForwarder(
            IStateStore store,
            IProcessor processor,
            Action<KeyValuePair<K, Change<ValueAndTimestamp<V>>>> listener,
            bool sendOldValues)
        {
            this.processor = processor;
            this.sendOldValues = sendOldValues;
            cachingEnabled = ((IWrappedStateStore)store).IsCachedStore &&
                             ((ICachedStateStore<K, ValueAndTimestamp<V>>)store).SetFlushListener(listener, sendOldValues);
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
            if (!cachingEnabled)
                processor?.Forward(key, new Change<V>(sendOldValues ? oldValue : default, newValue), timestamp);
        }

        public void MaybeForward<VR>(K key, VR newValue, VR oldValue, long timestamp)
        {
            if (!cachingEnabled)
                processor?.Forward(key, new Change<VR>(sendOldValues ? oldValue : default, newValue), timestamp);
        }
    }
}