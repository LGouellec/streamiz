using System;
using Streamiz.Kafka.Net.Crosscutting;
using System.Collections;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.State.Metered
{
    internal class MeteredWindowedKeyValueEnumerator<K, V>
        : IKeyValueEnumerator<Windowed<K>, V>
    {
        private readonly Sensor sensor;
        private readonly long startMs;
        private readonly IKeyValueEnumerator<Windowed<Bytes>, byte[]> innerEnumerator;
        private readonly Func<byte[], K> funcSerdesKey;
        private readonly Func<byte[], V> funcSerdesValue;

        public MeteredWindowedKeyValueEnumerator(
            IKeyValueEnumerator<Windowed<Bytes>, byte[]> keyValueEnumerator,
            Func<byte[], K> funcSerdesKey,
            Func<byte[], V> funcSerdesValue,
            Sensor sensor)
        {
            innerEnumerator = keyValueEnumerator;
            this.funcSerdesKey = funcSerdesKey;
            this.funcSerdesValue = funcSerdesValue;
            this.sensor = sensor;
            startMs = DateTime.Now.GetMilliseconds();
        }

        public KeyValuePair<Windowed<K>, V>? Current
        {
            get
            {
                var next = innerEnumerator.Current;
                if (next.HasValue)
                    return new KeyValuePair<Windowed<K>, V>(
                        WindowedKey(next.Value.Key),
                        funcSerdesValue(next.Value.Value));
                else
                    return null;
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            innerEnumerator.Dispose();
            sensor.Record(DateTime.Now.GetMilliseconds() - startMs);
        }

        public bool MoveNext() => innerEnumerator.MoveNext();

        public Windowed<K> PeekNextKey() => WindowedKey(innerEnumerator.PeekNextKey());

        public void Reset() => innerEnumerator.Reset();

        private Windowed<K> WindowedKey(Windowed<Bytes> bytesKey)
        {
            K key = funcSerdesKey(bytesKey.Key.Get);
            return new Windowed<K>(key, bytesKey.Window);
        }
    }
}