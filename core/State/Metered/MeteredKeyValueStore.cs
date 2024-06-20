using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Table.Internal;
using static Streamiz.Kafka.Net.Crosscutting.ActionHelper;

namespace Streamiz.Kafka.Net.State.Metered
{
    internal class MeteredKeyValueStore<K, V> :
        WrappedKeyValueStore<K, V>,
        IKeyValueStore<K, V>
    {
        private readonly string metricScope;
        private Sensor putSensor = NoRunnableSensor.Empty;
        private Sensor putIfAbsentSensor= NoRunnableSensor.Empty;
        private Sensor putAllSensor= NoRunnableSensor.Empty;
        private Sensor getSensor= NoRunnableSensor.Empty;
        private Sensor allSensor= NoRunnableSensor.Empty;
        private Sensor rangeSensor= NoRunnableSensor.Empty;
        private Sensor flushSensor= NoRunnableSensor.Empty;
        private Sensor deleteSensor= NoRunnableSensor.Empty;

        public MeteredKeyValueStore(
            IKeyValueStore<Bytes, byte[]> wrapped,
            ISerDes<K> keySerdes,
            ISerDes<V> valueSerdes,
            string metricScope) 
            : base(wrapped, keySerdes, valueSerdes)
        {
            this.metricScope = metricScope;
        }

        public override void Init(ProcessorContext context, IStateStore root)
        {
            base.Init(context, root);
            
            RegisterMetrics();
        }

        protected virtual void RegisterMetrics()
        {
            putSensor = StateStoreMetrics.PutSensor(context.Id, metricScope, Name, context.Metrics);
            putIfAbsentSensor = StateStoreMetrics.PutIfAbsentSensor(context.Id, metricScope, Name, context.Metrics);
            putAllSensor = StateStoreMetrics.PutAllSensor(context.Id, metricScope, Name, context.Metrics);
            getSensor = StateStoreMetrics.GetSensor(context.Id, metricScope, Name, context.Metrics);
            allSensor = StateStoreMetrics.AllSensor(context.Id, metricScope, Name, context.Metrics);
            rangeSensor = StateStoreMetrics.RangeSensor(context.Id, metricScope, Name, context.Metrics);
            flushSensor = StateStoreMetrics.FlushSensor(context.Id, metricScope, Name, context.Metrics);
            deleteSensor = StateStoreMetrics.DeleteSensor(context.Id, metricScope, Name, context.Metrics);
        }
        
        public override bool SetFlushListener(Action<KeyValuePair<K, Change<V>>> listener, bool sendOldChanges)
        {
            if (wrapped is ICachedStateStore<byte[], byte[]> store)
            {
                return store.SetFlushListener(
                    (kv) =>
                    {
                        var key = FromKey(Bytes.Wrap(kv.Key));
                        var newValue = kv.Value.NewValue != null ? FromValue(kv.Value.NewValue) : default;
                        var oldValue = kv.Value.OldValue != null ? FromValue(kv.Value.OldValue) : default;
                        listener(new KeyValuePair<K, Change<V>>(key, new Change<V>(oldValue, newValue)));
                    }, sendOldChanges);
            }
            return false;
        }

        // TODO : set theses methods in helper class, used in some part of the code
        private byte[] GetValueBytes(V value)
        {
            if(valueSerdes != null)
                return valueSerdes.Serialize(value, GetSerializationContext(false));
            else
                throw new StreamsException($"The serializer is not compatible to the actual value (Value type: {typeof(V).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }
        
        private V FromValue(byte[] values)
        {
            if(valueSerdes != null)
                return values != null ? valueSerdes.Deserialize(values, GetSerializationContext(false)) : default;
            else
                throw new StreamsException($"The serializer is not compatible to the actual value (Value type: {typeof(V).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private Bytes GetKeyBytes(K key)
        {
            if (keySerdes != null)
                return Bytes.Wrap(keySerdes.Serialize(key, GetSerializationContext(true)));
            else
                throw new StreamsException($"The serializer is not compatible to the actual key (Key type: {typeof(K).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }
        
        private K FromKey(Bytes key)
        {
            if(keySerdes != null)
                return keySerdes.Deserialize(key.Get, GetSerializationContext(true));
            else
                throw new StreamsException($"The serializer is not compatible to the actual key (Key type: {typeof(K).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private ProcessorStateException CaptureAndThrowException(ProcessorStateException e, K key)
        {
            var message = $"{e.Message}-key:{key}";
            return new ProcessorStateException(message, e);
        }
        
        private IKeyValueEnumerator<K, V> Range(Func<Bytes, Bytes, IKeyValueEnumerator<Bytes, byte[]>> enumerator, K @from, K to)
        {
            Bytes f = GetKeyBytes(@from);
            Bytes t = GetKeyBytes(to);
            
            return new MeteredKeyValueEnumerator<K, V>(
                enumerator(f, t),
                rangeSensor, 
                (b) => FromKey(Bytes.Wrap(b)),
                (b) => FromValue(b));
        }
        
        public V Get(K key)
        {
            try
            {
                return MeasureLatency(() => FromValue(wrapped.Get(GetKeyBytes(key))), getSensor);
            }
            catch (ProcessorStateException e)
            {
                throw CaptureAndThrowException(e, key);
            }
        }
        
        public IKeyValueEnumerator<K, V> Range(K @from, K to)
            => Range((f, t) => wrapped.Range(f, t), from, to);

        public IKeyValueEnumerator<K, V> ReverseRange(K @from, K to)
            => Range((f, t) => wrapped.ReverseRange(f, t), from, to);

        public IEnumerable<KeyValuePair<K, V>> All()
        {
            return MeasureLatency(() => wrapped
                .All()
                .Select(kv
                    => new KeyValuePair<K, V>(FromKey(kv.Key), FromValue(kv.Value)))
                .ToList(), allSensor);
        }

        public IEnumerable<KeyValuePair<K, V>> ReverseAll()
        {
            return MeasureLatency(() => wrapped
                .ReverseAll()
                .Select(kv
                    => new KeyValuePair<K, V>(FromKey(kv.Key), FromValue(kv.Value)))
                .ToList(), allSensor);
        }

        public long ApproximateNumEntries()
            => wrapped.ApproximateNumEntries();

        public void Put(K key, V value)
        {
            try
            {
                MeasureLatency(() => wrapped.Put(GetKeyBytes(key), GetValueBytes(value)), putSensor);
            }
            catch (ProcessorStateException e)
            {
                throw CaptureAndThrowException(e, key);
            }
        }

        public V PutIfAbsent(K key, V value)
        {
            try
            {
                return MeasureLatency(() => FromValue(wrapped.PutIfAbsent(GetKeyBytes(key), GetValueBytes(value))), putIfAbsentSensor);
            }
            catch (ProcessorStateException e)
            {
                throw CaptureAndThrowException(e, key);
            }
        }

        public void PutAll(IEnumerable<KeyValuePair<K, V>> entries)
        {
            var convertEntries = entries
                .Select(e => new KeyValuePair<Bytes, byte[]>(GetKeyBytes(e.Key), GetValueBytes(e.Value)));
            MeasureLatency(() => wrapped.PutAll(convertEntries), putAllSensor);
        }

        public V Delete(K key)
        {
            try
            {
                return MeasureLatency(() => FromValue(wrapped.Delete(GetKeyBytes(key))), deleteSensor);
            }
            catch (ProcessorStateException e)
            {
                throw CaptureAndThrowException(e, key);
            }
        }

        public override void Flush()
            => MeasureLatency(() => base.Flush(), flushSensor);

        public override void Close()
        {
            try
            {
                base.Close();
            }
            finally
            {
                context.Metrics.RemoveStoreSensors(
                    Thread.CurrentThread.Name, 
                    context.Id.ToString(),
                    Name);
            }
        }
    }
}