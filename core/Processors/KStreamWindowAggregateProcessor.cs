using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamWindowAggregateProcessor<K, V, Agg, W> : AbstractProcessor<K, V>
        where W : Window
    {
        private readonly bool sendOldValues;
        private readonly string storeName;
        private readonly WindowOptions<W> windowOptions;
        private readonly Initializer<Agg> initializer;
        private readonly Aggregator<K, V, Agg> aggregator;

        private long observedStreamTime = -1;
        private TimestampedWindowStore<K, Agg> windowStore;
        private TimestampedTupleForwarder<Windowed<K>, Agg> tupleForwarder;

        public KStreamWindowAggregateProcessor()
        {
        }


        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            windowStore = (TimestampedWindowStore<K, Agg>)context.GetStateStore(storeName);
            tupleForwarder = new TimestampedTupleForwarder<Windowed<K>, Agg>(this, sendOldValues);
        }

        public override void Process(K key, V value)
        {
            if (key == null)
            {
                log.Warn($"Skipping record due to null key.value =[{value}] topic =[{Context.RecordContext.Topic}] partition =[{Context.RecordContext.Partition}] offset =[{Context.RecordContext.Offset }]");
                return;
            }

            observedStreamTime = Math.Max(observedStreamTime, Context.Timestamp);
            long closeTime = observedStreamTime - windowOptions.GracePeriodMs;
            var matchedWindows = windowOptions.WindowsFor(Context.Timestamp);

            foreach (var entry in matchedWindows)
            {
                long windowStart = entry.Key, windowEnd = entry.Value.EndMs;
                if(windowEnd > closeTime)
                {
                    var oldAggAndTimestamp = windowStore.Fetch(key, windowStart);
                    Agg oldAgg = oldAggAndTimestamp == null ? default : oldAggAndTimestamp.Value;
                    long newTs;
                    Agg newAgg;

                    if (oldAgg == null)
                    {
                        oldAgg = initializer.Apply();
                        newTs = Context.Timestamp;
                    }
                    else
                        newTs = Math.Max(Context.Timestamp, oldAggAndTimestamp.Timestamp);

                    newAgg = aggregator.Apply(key, value, oldAgg);
                    windowStore.Put(key, ValueAndTimestamp<Agg>.Make(newAgg, newTs), windowStart);
                    tupleForwarder.MaybeForward(new Windowed<K>(key, entry.Value), newAgg, sendOldValues ? oldAgg : default, newTs);
                }
                else
                {
                    log.Debug($"Skipping record for expired window. key=[{key}] topic=[{Context.Topic}] partition=[{Context.Partition}] offset=[{Context.Offset}] " +
                            $"timestamp=[{Context.Timestamp}] window=[{windowStart},{windowEnd}) " +
                            $"expiration=[{closeTime}] streamTime=[{observedStreamTime}]");
                }
            }
        }
    }
}
