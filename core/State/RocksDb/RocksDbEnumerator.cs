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
        private Iterator iterator;
        private readonly string name;

        public RocksDbEnumerator(Iterator iterator, string name)
        {
            this.iterator = iterator;
            this.name = name;
        }

        public KeyValuePair<Bytes, byte[]>? Current { get; private set; } = null;

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            iterator.Detach();
            iterator.Dispose();
        }

        public bool MoveNext()
        {
            if (iterator.Valid())
            {
                Current = new KeyValuePair<Bytes, byte[]>(new Bytes(iterator.Key()), iterator.Value());
                iterator = iterator.Next();
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
            => Dispose();
    }
}