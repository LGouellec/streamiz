using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.State.Logging
{
    internal class ChangeLoggingKeyValueBytesStore:
        WrappedStateStore<IKeyValueStore<Bytes, byte[]>>,
        IKeyValueStore<Bytes, byte[]>
    {
        public ChangeLoggingKeyValueBytesStore(IKeyValueStore<Bytes, byte[]> wrapped) 
            : base(wrapped)
        {
        }

        public override bool IsCachedStore => false;
        
        protected virtual void Publish(Bytes key, byte[] value)
            => context.Log(Name, key, value, context.RecordContext.Timestamp);

        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
            => wrapped.All();

        public long ApproximateNumEntries()
            => wrapped.ApproximateNumEntries();

        public byte[] Delete(Bytes key)
        {
            byte[] oldValue = wrapped.Delete(key);
            Publish(key, null);
            return oldValue;
        }

        public byte[] Get(Bytes key)
            => wrapped.Get(key);

        public void Put(Bytes key, byte[] value)
        {
            wrapped.Put(key, value);
            Publish(key, value);
        }

        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            wrapped.PutAll(entries);
            foreach (var e in entries)
            {
                Publish(e.Key, e.Value);
            }
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            var previous = wrapped.PutIfAbsent(key, value);
            if (previous == null)
            {
                Publish(key, value);
            }

            return previous;
        }

        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
            => wrapped.Range(from, to);

        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
            => wrapped.ReverseAll();

        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
            => wrapped.ReverseRange(from, to);
    }
}