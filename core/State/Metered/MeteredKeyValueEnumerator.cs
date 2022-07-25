using System;
using System.Collections;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.State.Metered
{
    internal class MeteredKeyValueEnumerator<K, V> 
        : IKeyValueEnumerator<K, V>
    {
        private readonly IKeyValueEnumerator<Bytes, byte[]> innerEnumerator;
        private readonly Sensor sensor;
        private readonly Func<byte[], K> funcSerdesKey;
        private readonly Func<byte[], V> funcSerdesValue;
        private readonly long startMs;

        public MeteredKeyValueEnumerator(
            IKeyValueEnumerator<Bytes, byte[]> innerEnumerator,
            Sensor sensor,
            Func<byte[], K> funcSerdesKey,
            Func<byte[], V> funcSerdesValue)
        {
            this.innerEnumerator = innerEnumerator;
            this.sensor = sensor;
            this.funcSerdesKey = funcSerdesKey;
            this.funcSerdesValue = funcSerdesValue;
            startMs = DateTime.Now.GetMilliseconds();
        }
        
        public K PeekNextKey()
        {
            var k = innerEnumerator.PeekNextKey();
            return funcSerdesKey(k.Get);
        }

        public bool MoveNext()
            => innerEnumerator.MoveNext();

        public void Reset()
            => innerEnumerator.Reset();

        public KeyValuePair<K, V>? Current
        {
            get
            {
                var item = innerEnumerator.Current;
                return new KeyValuePair<K, V>(funcSerdesKey(item.Value.Key.Get), funcSerdesValue(item.Value.Value));
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            innerEnumerator.Dispose();
            sensor.Record(DateTime.Now.GetMilliseconds() - startMs);
        }
    }
}