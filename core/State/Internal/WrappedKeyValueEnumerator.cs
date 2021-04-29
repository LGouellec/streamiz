using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Enumerator;
using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class WrappedKeyValueEnumerator<K, V> :
        IKeyValueEnumerator<K, V>
    {
        private readonly IKeyValueEnumerator<Bytes, byte[]> enumerator;
        private readonly ISerDes<K> keySerdes;
        private readonly ISerDes<V> valueSerdes;

        public WrappedKeyValueEnumerator(IKeyValueEnumerator<Bytes, byte[]> enumerator, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            this.enumerator = enumerator;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }

        public KeyValuePair<K, V>? Current
        {
            get
            {
                if (enumerator.Current.HasValue)
                {
                    return KeyValuePair.Create(
                        keySerdes.Deserialize(enumerator.Current.Value.Key.Get, new Confluent.Kafka.SerializationContext()),
                        valueSerdes.Deserialize(enumerator.Current.Value.Value, new Confluent.Kafka.SerializationContext()));
                }
                else
                {
                    return null;
                }
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose()
            => enumerator.Dispose();

        public bool MoveNext()
            => enumerator.MoveNext();

        public K PeekNextKey()
            => keySerdes.Deserialize(enumerator.PeekNextKey().Get, new Confluent.Kafka.SerializationContext());

        public void Reset()
            => enumerator.Reset();
    }
}
