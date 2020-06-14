using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    internal class WindowStoreEnumerator<V> : IWindowStoreEnumerator<V>
    {
        private readonly IWindowStoreEnumerator<byte[]> innerEnumerator;
        private readonly ISerDes<V> serdes;

        public WindowStoreEnumerator(IWindowStoreEnumerator<byte[]> innerEnumerator, ISerDes<V> serdes)
        {
            this.innerEnumerator = innerEnumerator;
            this.serdes = serdes;
        }

        public KeyValuePair<long, V> Current
        {
            get
            {
                var next = innerEnumerator.Current;
                return KeyValuePair.Create(next.Key, serdes.Deserialize(next.Value));
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            innerEnumerator.Dispose();
        }

        public bool MoveNext() => innerEnumerator.MoveNext();

        public long PeekNextKey() => innerEnumerator.PeekNextKey();

        public void Reset() => innerEnumerator.Reset();

        
    }
}
