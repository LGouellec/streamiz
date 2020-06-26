using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    internal class CompositeKeyValueEnumerator<K, V, S> : IKeyValueEnumerator<K, V>
    {
        private readonly List<S> storeIterator;
        private readonly Func<S, IKeyValueEnumerator<K, V>> nextIterator;
        private IKeyValueEnumerator<K, V> current;
        private int index = 0;

        public CompositeKeyValueEnumerator(
            IEnumerable<S> storeEnumerable,
            Func<S, IKeyValueEnumerator<K, V>> nextIterator)
        {
            storeIterator = new List<S>(storeEnumerable);
            this.nextIterator = nextIterator;
            current = null;
        }

        public KeyValuePair<K, V>? Current => current.Current;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            CloseCurrentEnumerator();
        }

        private void CloseCurrentEnumerator()
        {
            current?.Dispose();
            current = null;
        }

        public bool MoveNext()
        {
            while ((current == null || !current.MoveNext()) && index < storeIterator.Count)
            {
                CloseCurrentEnumerator();
                current = nextIterator?.Invoke(storeIterator[index]);
                ++index;
            }

            return current != null && current.Current.HasValue;
        }

        public K PeekNextKey()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            CloseCurrentEnumerator();
            index = 0;
        }
    }
}
