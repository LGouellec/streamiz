using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State.Enumerator;
using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State
{
    internal class RocksDbEnumerator : IKeyValueEnumerator<Bytes, byte[]>
    {
        protected Iterator iterator;
        protected readonly string name;
        protected readonly bool forward;
        private bool firstMoveNext = true;

        public RocksDbEnumerator(Iterator iterator, string name, bool forward)
        {
            this.iterator = iterator;
            this.name = name;
            this.forward = forward;
        }

        public KeyValuePair<Bytes, byte[]>? Current { get; protected set; } = null;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            iterator.Detach();
            iterator.Dispose();
        }

        public virtual bool MoveNext()
        {
            if (!firstMoveNext)
                iterator = forward ? iterator.Next() : iterator.Prev();
            else
                firstMoveNext = false;

            if (iterator.Valid())
            {
                Current = new KeyValuePair<Bytes, byte[]>(new Bytes(iterator.Key()), iterator.Value());
                return true;
            }
            else
            {
                Current = null;
                return false;
            }
        }

        public Bytes PeekNextKey()
            => Current?.Key;

        public void Reset()
        {
            firstMoveNext = true;
            iterator = forward ? iterator.SeekToFirst() : iterator.SeekToLast();
        }
    }
}