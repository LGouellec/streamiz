using System;
using System.Threading;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Suppress;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KTableSuppressProcessor<K, V> : AbstractProcessor<K, Change<V>>
    {
        private readonly long maxRecords;
        private readonly long maxBytes;
        private readonly long suppressionDurationMs;
        private readonly BUFFER_FULL_STRATEGY bufferFullStrategy;
        private readonly bool safeToDropTombstone;
        private readonly string storeName;
        private readonly Func<ProcessorContext, K, long> timeDefinition;
        
        private Sensor suppressionEmitSensor;
        private long observedStreamTime = -1;
        private ITimeOrderedKeyValueBuffer<K, V, Change<V>> buffer;
        private readonly ISerDes<K> keySerdes;
        private readonly ISerDes<V> valueSerdes;

        private bool OverCapacity => buffer.BufferSize > maxBytes || buffer.NumRecords > maxRecords;

        public KTableSuppressProcessor(Suppressed<K, V> suppressed, string storeName)
        {
            maxRecords = suppressed.BufferConfig.MaxRecords;
            maxBytes = suppressed.BufferConfig.MaxBytes;
            bufferFullStrategy = suppressed.BufferConfig.BufferFullStrategy;
            suppressionDurationMs = (long)suppressed.SuppressionTime.TotalMilliseconds;
            safeToDropTombstone = suppressed.SafeToDropTombstones;
            this.storeName = storeName;
            timeDefinition = suppressed.TimeDefinition;
            keySerdes = suppressed.KeySerdes;
            valueSerdes = suppressed.ValueSerdes;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);
            suppressionEmitSensor = ProcessorNodeMetrics.SuppressionEmitSensor(
                Thread.CurrentThread.Name,
                context.Id,
                Name,
                context.Metrics);
            
            buffer = (ITimeOrderedKeyValueBuffer<K, V, Change<V>>)context.GetStateStore(storeName);
            buffer.SetSerdesIfNull(keySerdes, valueSerdes);
        }

        public override void Process(K key, Change<V> value)
        {
            observedStreamTime = Math.Max(observedStreamTime, Context.Timestamp);
            Buffer(key, value);
            EnforceConstraints();
        }

        private void Buffer(K key, Change<V> value)
        {
            var bufferTime = timeDefinition(Context, key);
            buffer.Put(bufferTime, key, value, Context.RecordContext);
        }

        private void EnforceConstraints()
        {
            long expiryTime = observedStreamTime - suppressionDurationMs;
            buffer.EvictWhile(() => buffer.MinTimestamp <= expiryTime, Emit);

            if (OverCapacity)
            {
                switch (bufferFullStrategy)
                {
                    case BUFFER_FULL_STRATEGY.EMIT:
                        buffer.EvictWhile(() => OverCapacity, Emit);
                        break;
                    case BUFFER_FULL_STRATEGY.SHUTDOWN:
                        throw new StreamsException(
                            $"{Name} buffer exceeded its max capacity. Currently [{buffer.NumRecords}/{maxRecords}] records and [{buffer.BufferSize}/{maxBytes}] bytes.");
                }
            }
        }

        private void Emit(K key, Change<V> value, IRecordContext context)
        {
            if (value.NewValue != null || !safeToDropTombstone)
            {
                var currentRecordContext = Context.RecordContext;
                try
                {
                    Context.SetRecordMetaData(context);
                    Forward(key, value);
                    suppressionEmitSensor.Record();
                }
                finally
                {
                    Context.SetRecordMetaData(currentRecordContext);
                }
            }
        }
    }
}