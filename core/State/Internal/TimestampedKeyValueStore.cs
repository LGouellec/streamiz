using kafka_stream_core.Crosscutting;
using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State.Internal
{
    internal class TimestampedKeyValueStore<K, V> :
        WrappedStateStore<KeyValueStore<Bytes, byte[]>, K, V>,
        kafka_stream_core.State.TimestampedKeyValueStore<K, V>
    {
        private readonly ISerDes<K> keySerdes;
        private readonly ISerDes<ValueAndTimestamp<V>> valueSerdes;

        public TimestampedKeyValueStore(KeyValueStore<Bytes, byte[]> wrapped, ISerDes<K> keySerdes, ISerDes<ValueAndTimestamp<V>> valueSerdes) 
            : base(wrapped)
        {
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }

        private Bytes GetKeyBytes(K key) => new Bytes(this.keySerdes.Serialize(key));
        private byte[] GetValueBytes(ValueAndTimestamp<V> value) => this.valueSerdes.Serialize(value);
        private ValueAndTimestamp<V> FromValue(byte[] values) => values != null ? this.valueSerdes.Deserialize(values) : null;

        #region TimestampedKeyValueStore Impl

        public long approximateNumEntries() => this.wrapped.approximateNumEntries();

        public ValueAndTimestamp<V> delete(K key) => FromValue(wrapped.delete(GetKeyBytes(key)));

        public ValueAndTimestamp<V> get(K key) => FromValue(wrapped.get(GetKeyBytes(key)));

        public void put(K key, ValueAndTimestamp<V> value) => wrapped.put(GetKeyBytes(key), GetValueBytes(value));

        public void putAll(IEnumerable<KeyValuePair<K, ValueAndTimestamp<V>>> entries)
        {
            foreach (var kp in entries)
                put(kp.Key, kp.Value);
        }

        public ValueAndTimestamp<V> putIfAbsent(K key, ValueAndTimestamp<V> value)
            => FromValue(wrapped.putIfAbsent(GetKeyBytes(key), GetValueBytes(value)));

        #endregion
    }
}
