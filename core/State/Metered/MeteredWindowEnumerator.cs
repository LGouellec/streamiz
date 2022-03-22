using System;
using System.Collections;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.State.Metered
{
    internal class MeteredWindowEnumerator<V> 
        : IWindowStoreEnumerator<V>
    {
        private readonly IWindowStoreEnumerator<byte[]> innerEnumerator;
        private readonly Func<byte[], V> funcSerdesValue;
        private readonly Sensor sensor;
        private readonly long startMs;

        public MeteredWindowEnumerator(
            IWindowStoreEnumerator<byte[]> innerEnumerator,
            Func<byte[], V> funcSerdesValue,
            Sensor sensor)
        {
            this.innerEnumerator = innerEnumerator;
            this.funcSerdesValue = funcSerdesValue;
            this.sensor = sensor;
            startMs = DateTime.Now.GetMilliseconds();
        }

        public long PeekNextKey()
            => innerEnumerator.PeekNextKey();

        public bool MoveNext()
            => innerEnumerator.MoveNext();

        public void Reset()
            => innerEnumerator.Reset();

        public KeyValuePair<long, V>? Current
        {
            get
            {
                var next = innerEnumerator.Current;
                if (next.HasValue)
                    return KeyValuePair.Create(next.Value.Key, funcSerdesValue(next.Value.Value));
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
    }
}