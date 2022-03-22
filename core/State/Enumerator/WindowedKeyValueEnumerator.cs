using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    internal class WindowedKeyValueEnumerator<K, V> : IKeyValueEnumerator<Windowed<K>, V>
    {
        private readonly IKeyValueEnumerator<Windowed<Bytes>, byte[]> innerEnumerator;
        private readonly ISerDes<K> keySerdes;
        private readonly ISerDes<V> valueSerdes;

        public WindowedKeyValueEnumerator(
            IKeyValueEnumerator<Windowed<Bytes>, byte[]> keyValueEnumerator,
            ISerDes<K> keySerdes,
            ISerDes<V> valueSerdes)
        {
            innerEnumerator = keyValueEnumerator;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }

        public KeyValuePair<Windowed<K>, V>? Current
        {
            get
            {
                var next = innerEnumerator.Current;
                if (next.HasValue)
                    return KeyValuePair.Create(WindowedKey(next.Value.Key), valueSerdes.Deserialize(next.Value.Value, new Confluent.Kafka.SerializationContext()));
                else
                    return null;
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose() => innerEnumerator.Dispose();

        public bool MoveNext() => innerEnumerator.MoveNext();

        public Windowed<K> PeekNextKey() => WindowedKey(innerEnumerator.PeekNextKey());

        public void Reset() => innerEnumerator.Reset();

        private Windowed<K> WindowedKey(Windowed<Bytes> bytesKey)
        {
            K key = keySerdes.Deserialize(bytesKey.Key.Get, new Confluent.Kafka.SerializationContext());
            return new Windowed<K>(key, bytesKey.Window);
        }
    }
}
