using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State.Enumerator;
using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    public class RocksDbEnumerable : IEnumerable<KeyValuePair<Bytes, byte[]>>
    {
        #region Inner Class
        private class RocksDbWrappedEnumerator : IEnumerator<KeyValuePair<Bytes, byte[]>>
        {
            private readonly string stateStoreName;
            private readonly IKeyValueEnumerator<Bytes, byte[]> enumerator;

            public RocksDbWrappedEnumerator(string stateStoreName, IKeyValueEnumerator<Bytes, byte[]> enumerator)
            {
                this.stateStoreName = stateStoreName;
                this.enumerator = enumerator;
            }

            public KeyValuePair<Bytes, byte[]> Current 
                => enumerator.Current.HasValue ? 
                        enumerator.Current.Value :
                        throw new NotMoreValueException($"No more record present in your state store {stateStoreName}");

            object IEnumerator.Current => Current;

            public void Dispose()
                => enumerator.Dispose();

            public bool MoveNext()
                => enumerator.MoveNext();

            public void Reset()
                => enumerator.Reset();
        }

        #endregion

        private readonly IKeyValueEnumerator<Bytes, byte[]> enumerator;
        private readonly string stateStoreName;

        public RocksDbEnumerable(string stateStoreName, IKeyValueEnumerator<Bytes, byte[]> enumerator)
        {
            this.stateStoreName = stateStoreName;
            this.enumerator = enumerator;
        }

        public IEnumerator<KeyValuePair<Bytes, byte[]>> GetEnumerator()
            => new RocksDbWrappedEnumerator(stateStoreName, enumerator);

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
    }

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
            => Current.HasValue ? Current.Value.Key : null;

        public void Reset()
        {
            firstMoveNext = true;
            iterator = forward ? iterator.SeekToFirst() : iterator.SeekToLast();
        }
    }
}