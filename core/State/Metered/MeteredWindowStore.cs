using System;
using System.Collections.Generic;
using System.Threading;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Helper;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Table.Internal;
using static Streamiz.Kafka.Net.Crosscutting.ActionHelper;

namespace Streamiz.Kafka.Net.State.Metered
{
    internal class MeteredWindowStore<K, V> :
        WrappedStateStore<IWindowStore<Bytes, byte[]>>,
        IWindowStore<K, V>,
        ICachedStateStore<Windowed<K>, V>
    {
        private readonly long windowSizeMs;
        protected ISerDes<K> keySerdes;
        protected ISerDes<V> valueSerdes;
        private readonly string metricScope;
        protected bool initStoreSerdes = false;
        
        private Sensor putSensor = NoRunnableSensor.Empty;
        private Sensor fetchSensor = NoRunnableSensor.Empty;
        private Sensor flushSensor = NoRunnableSensor.Empty;

        public MeteredWindowStore(
            IWindowStore<Bytes, byte[]> wrapped, 
            long windowSizeMs,
            ISerDes<K> keySerdes,
            ISerDes<V> valueSerdes,
            string metricScope)
            : base(wrapped)
        {
            this.windowSizeMs = windowSizeMs;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
            this.metricScope = metricScope;
        }
        
        public override bool IsCachedStore => ((IWrappedStateStore)wrapped).IsCachedStore;

        public virtual void InitStoreSerde(ProcessorContext context)
        {
            if (!initStoreSerdes)
            {
                keySerdes ??= context.Configuration.DefaultKeySerDes as ISerDes<K>;
                valueSerdes ??= context.Configuration.DefaultValueSerDes as ISerDes<V>;

                keySerdes?.Initialize(new SerDesContext(context.Configuration));
                valueSerdes?.Initialize(new SerDesContext(context.Configuration));
                
                initStoreSerdes = true;
            }
        }
        
        public override void Init(ProcessorContext context, IStateStore root)
        {
            base.Init(context, root);
            InitStoreSerde(context);
            RegisterMetrics();
        }

        protected virtual void RegisterMetrics()
        {
            putSensor = StateStoreMetrics.PutSensor(context.Id, metricScope, Name, context.Metrics);
            fetchSensor = StateStoreMetrics.FetchSensor(context.Id, metricScope, Name, context.Metrics);
            flushSensor = StateStoreMetrics.FlushSensor(context.Id, metricScope, Name, context.Metrics);
        }
        
        public bool SetFlushListener(Action<KeyValuePair<Windowed<K>, Change<V>>> listener, bool sendOldChanges)
        {
            if (wrapped is ICachedStateStore<byte[], byte[]> store)
            {
                return store.SetFlushListener(
                    kv => {
                        var key = WindowKeyHelper.FromStoreKey(kv.Key, windowSizeMs, keySerdes, changelogTopic);
                        var newValue = kv.Value.NewValue != null ? FromValue(kv.Value.NewValue) : default;
                        var oldValue = kv.Value.OldValue != null ? FromValue(kv.Value.OldValue) : default;
                        listener(new KeyValuePair<Windowed<K>, Change<V>>(key, new Change<V>(oldValue, newValue)));
                    }, sendOldChanges);
            }
            return false;
        }
        
        private Bytes GetKeyBytes(K key)
        {
            if (keySerdes != null)
                return new Bytes(keySerdes.Serialize(key, GetSerializationContext(true)));
            else
                throw new StreamsException($"The serializer is not compatible to the actual key (Key type: {typeof(K).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }
        
        private K FromKey(byte[] key)
        {
            if (keySerdes != null)
                return keySerdes.Deserialize(key, GetSerializationContext(true));
            else
                throw new StreamsException($"The serializer is not compatible to the actual key (Key type: {typeof(K).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private byte[] GetValueBytes(V value)
        {
            if (valueSerdes != null)
                return valueSerdes.Serialize(value, GetSerializationContext(false));
            else
                throw new StreamsException($"The serializer is not compatible to the actual value (Value type: {typeof(V).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private V FromValue(byte[] values)
        {
            if (valueSerdes != null)
                return values != null ? valueSerdes.Deserialize(values, GetSerializationContext(false)) : default;
            else
                throw new StreamsException($"The serializer is not compatible to the actual value (Value type: {typeof(V).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }
        
        private ProcessorStateException CaptureAndThrowException(ProcessorStateException e, K key)
        {
            var message = $"{e.Message}-key:{key}";
            return new ProcessorStateException(message, e);
        }
        
        public V Fetch(K key, long time)
        {
            return MeasureLatency(() => FromValue(wrapped.Fetch(GetKeyBytes(key), time)), fetchSensor);
        }
        
        public IWindowStoreEnumerator<V> Fetch(K key, DateTime from, DateTime to)
            => new MeteredWindowEnumerator<V>(
                wrapped.Fetch(GetKeyBytes(key), from, to),
                (b) => FromValue(b),
                fetchSensor);

        public IWindowStoreEnumerator<V> Fetch(K key, long from, long to)
            => new MeteredWindowEnumerator<V>(
                wrapped.Fetch(GetKeyBytes(key), from, to),
                (b) => FromValue(b),
                fetchSensor);
        

        public IKeyValueEnumerator<Windowed<K>, V> FetchAll(DateTime from, DateTime to)
            => new MeteredWindowedKeyValueEnumerator<K, V>(
                wrapped.FetchAll(from, to),
                (b) => FromKey(b),
                (b) => FromValue(b),
                fetchSensor);

        public IKeyValueEnumerator<Windowed<K>, V> All()
            => new MeteredWindowedKeyValueEnumerator<K, V>(
                wrapped.All(),
                (b) => FromKey(b),
                (b) => FromValue(b),
                fetchSensor);

        public void Put(K key, V value, long windowStartTimestamp)
        {
            try
            {
                MeasureLatency(() => wrapped.Put(GetKeyBytes(key), GetValueBytes(value), windowStartTimestamp), putSensor);
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