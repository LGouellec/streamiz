using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class TimestampedKeyValueStoreImpl<K, V> :
        WrappedKeyValueStore<K, ValueAndTimestamp<V>>,
        TimestampedKeyValueStore<K, V>
    {
        private bool initStoreSerdes = false;

        public TimestampedKeyValueStoreImpl(IKeyValueStore<Bytes, byte[]> wrapped, ISerDes<K> keySerdes, ISerDes<ValueAndTimestamp<V>> valueSerdes)
            : base(wrapped, keySerdes, valueSerdes)
        {

        }

        private Bytes GetKeyBytes(K key)
        {
            if (keySerdes != null)
                return new Bytes(keySerdes.Serialize(key));
            else
                throw new StreamsException($"The serializer is not compatible to the actual key (Key type: {typeof(K).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private byte[] GetValueBytes(ValueAndTimestamp<V> value)
        {
            if(valueSerdes != null)
                return valueSerdes.Serialize(value);
            else
                throw new StreamsException($"The serializer is not compatible to the actual value (Value type: {typeof(V).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private ValueAndTimestamp<V> FromValue(byte[] values)
        {
            if(valueSerdes != null)
                return values != null ? valueSerdes.Deserialize(values) : null;
            else
                throw new StreamsException($"The serializer is not compatible to the actual value (Value type: {typeof(V).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        private K FromKey(Bytes key)
        {
            if(keySerdes != null)
                return keySerdes.Deserialize(key.Get);
            else
                throw new StreamsException($"The serializer is not compatible to the actual key (Key type: {typeof(K).FullName}). Change the default Serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
        }

        #region TimestampedKeyValueStore Impl

        public long ApproximateNumEntries() => wrapped.ApproximateNumEntries();

        public ValueAndTimestamp<V> Delete(K key) => FromValue(wrapped.Delete(GetKeyBytes(key)));

        public ValueAndTimestamp<V> Get(K key) => FromValue(wrapped.Get(GetKeyBytes(key)));

        public void Put(K key, ValueAndTimestamp<V> value) => wrapped.Put(GetKeyBytes(key), GetValueBytes(value));

        public IEnumerable<KeyValuePair<K, ValueAndTimestamp<V>>> All()
        {
            foreach (var keyValuePair in wrapped.All())
            {
                yield return new KeyValuePair<K, ValueAndTimestamp<V>>(FromKey(keyValuePair.Key), FromValue(keyValuePair.Value));
            }
        }

        public void PutAll(IEnumerable<KeyValuePair<K, ValueAndTimestamp<V>>> entries)
        {
            foreach (var kp in entries)
                Put(kp.Key, kp.Value);
        }

        public ValueAndTimestamp<V> PutIfAbsent(K key, ValueAndTimestamp<V> value)
            => FromValue(wrapped.PutIfAbsent(GetKeyBytes(key), GetValueBytes(value)));

        #endregion

        public override void InitStoreSerDes(ProcessorContext context)
        {
            if (!initStoreSerdes)
            {
                keySerdes = keySerdes == null ? context.Configuration.DefaultKeySerDes as ISerDes<K> : keySerdes;
                valueSerdes = valueSerdes == null ? new ValueAndTimestampSerDes<V>(context.Configuration.DefaultValueSerDes as ISerDes<V>) : valueSerdes;
                initStoreSerdes = true;
            }
        }
    }
}
