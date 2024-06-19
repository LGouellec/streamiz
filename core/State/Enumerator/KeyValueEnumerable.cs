using System.Collections;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.State.Enumerator
{
    internal class KeyValueEnumerable : IEnumerable<KeyValuePair<Bytes, byte[]>>
    {
        #region Inner Class
        private class WrappedEnumerator : IEnumerator<KeyValuePair<Bytes, byte[]>>
        {
            private readonly string stateStoreName;
            private readonly IKeyValueEnumerator<Bytes, byte[]> enumerator;

            public WrappedEnumerator(string stateStoreName, IKeyValueEnumerator<Bytes, byte[]> enumerator)
            {
                this.stateStoreName = stateStoreName;
                this.enumerator = enumerator;
            }

            public KeyValuePair<Bytes, byte[]> Current 
                => enumerator.Current ?? throw new NotMoreValueException($"No more record present in your state store {stateStoreName}");

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

        public KeyValueEnumerable(string stateStoreName, IKeyValueEnumerator<Bytes, byte[]> enumerator)
        {
            this.stateStoreName = stateStoreName;
            this.enumerator = enumerator;
        }

        public IEnumerator<KeyValuePair<Bytes, byte[]>> GetEnumerator()
            => new WrappedEnumerator(stateStoreName, enumerator);

        IEnumerator IEnumerable.GetEnumerator()
            => GetEnumerator();
    }
}