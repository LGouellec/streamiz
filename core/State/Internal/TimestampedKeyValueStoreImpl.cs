using Kafka.Streams.Net.Crosscutting;
using Kafka.Streams.Net.SerDes;
using System.Collections.Generic;

namespace Kafka.Streams.Net.State.Internal
{
    internal class TimestampedKeyValueStoreImpl<K, V> :
        WrappedKeyValueStore<K, ValueAndTimestamp<V>>,
        TimestampedKeyValueStore<K, V>
    {
        private bool initStoreSerdes = false;

        public TimestampedKeyValueStoreImpl(KeyValueStore<Bytes, byte[]> wrapped, ISerDes<K> keySerdes, ISerDes<ValueAndTimestamp<V>> valueSerdes)
            : base(wrapped, keySerdes, valueSerdes)
        {

        }

        private Bytes GetKeyBytes(K key) => new Bytes(this.keySerdes.Serialize(key));
        private byte[] GetValueBytes(ValueAndTimestamp<V> value) => this.valueSerdes.Serialize(value);
        private ValueAndTimestamp<V> FromValue(byte[] values) => values != null ? this.valueSerdes.Deserialize(values) : null;

        #region TimestampedKeyValueStore Impl

        public long ApproximateNumEntries() => this.wrapped.ApproximateNumEntries();

        public ValueAndTimestamp<V> Delete(K key) => FromValue(wrapped.Delete(GetKeyBytes(key)));

        public ValueAndTimestamp<V> Get(K key) => FromValue(wrapped.Get(GetKeyBytes(key)));

        public void Put(K key, ValueAndTimestamp<V> value) => wrapped.Put(GetKeyBytes(key), GetValueBytes(value));

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
