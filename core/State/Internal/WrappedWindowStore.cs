using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Enumerator;
using System;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class WrappedWindowStore<K, V> :
        WrappedStateStore<IWindowStore<Bytes, byte[]>>, IWindowStore<K, V>
    {
        private readonly long windowSizeMs;
        protected ISerDes<K> keySerdes;
        protected ISerDes<V> valueSerdes;

        public WrappedWindowStore(IWindowStore<Bytes, byte[]> wrapped, long windowSizeMs, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : base(wrapped)
        {
            this.windowSizeMs = windowSizeMs;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }

        public virtual void InitStoreSerde(ProcessorContext context)
        {
            keySerdes = keySerdes == null ? context.Configuration.DefaultKeySerDes as ISerDes<K> : keySerdes;
            valueSerdes = valueSerdes == null ? context.Configuration.DefaultValueSerDes as ISerDes<V> : valueSerdes;
        }

        public override void Init(ProcessorContext context, IStateStore root)
        {
            base.Init(context, root);
            InitStoreSerde(context);
        }

        private Bytes GetKeyBytes(K key)
        {
            if (keySerdes != null)
                return new Bytes(keySerdes.Serialize(key, GetSerializationContext(true)));
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

        public V Fetch(K key, long time)
            => FromValue(wrapped.Fetch(GetKeyBytes(key), time));

        public IWindowStoreEnumerator<V> Fetch(K key, DateTime from, DateTime to)
           => new WindowStoreEnumerator<V>(wrapped.Fetch(GetKeyBytes(key), from, to), valueSerdes);

        public IWindowStoreEnumerator<V> Fetch(K key, long from, long to)
            => new WindowStoreEnumerator<V>(wrapped.Fetch(GetKeyBytes(key), from, to), valueSerdes);

        public IKeyValueEnumerator<Windowed<K>, V> FetchAll(DateTime from, DateTime to)
            => new WindowedKeyValueEnumerator<K, V>(wrapped.FetchAll(from, to), keySerdes, valueSerdes);

        public IKeyValueEnumerator<Windowed<K>, V> All()
            => new WindowedKeyValueEnumerator<K, V>(wrapped.All(), keySerdes, valueSerdes);

        public void Put(K key, V value, long windowStartTimestamp)
            => wrapped.Put(GetKeyBytes(key), GetValueBytes(value), windowStartTimestamp);
    }
}
