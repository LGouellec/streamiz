using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using System;

namespace Streamiz.Kafka.Net.State.RocksDb.Internal
{
    internal class RocksDbSegmentedBytesStore<S> : ISegmentedBytesStore
        where S : ISegment
    {
        private readonly ISegments<S> segments;
        private readonly IKeySchema keySchema;
        private bool isOpen = false;

        public RocksDbSegmentedBytesStore(
            string name,
            long retention,
            long segmentInterval,
            IKeySchema keySchema)
        {
            this.keySchema = keySchema;
            Name = name;
        }

        #region ISegmentedBytesStore Impl

        public string Name { get; }

        public bool Persistent => true;

        public bool IsOpen => isOpen;

        public IKeyValueEnumerator<Bytes, byte[]> All()
        {
            throw new NotImplementedException();
        }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public IKeyValueEnumerator<Bytes, byte[]> Fetch(Bytes key, long from, long to)
        {
            throw new NotImplementedException();
        }

        public IKeyValueEnumerator<Bytes, byte[]> Fetch(Bytes fromKey, Bytes toKey, long from, long to)
        {
            throw new NotImplementedException();
        }

        public IKeyValueEnumerator<Bytes, byte[]> FetchAll(long from, long to)
        {
            throw new NotImplementedException();
        }

        public void Flush()
        {
            throw new NotImplementedException();
        }

        public byte[] Get(Bytes key)
        {
            throw new NotImplementedException();
        }

        public void Init(ProcessorContext context, IStateStore root)
        {
            throw new NotImplementedException();
        }

        public void Put(Bytes key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public void Remove(Bytes key)
        {
            throw new NotImplementedException();
        }

        public IKeyValueEnumerator<Bytes, byte[]> ReverseAll()
        {
            throw new NotImplementedException();
        }

        public IKeyValueEnumerator<Bytes, byte[]> ReverseFetch(Bytes key, long from, long to)
        {
            throw new NotImplementedException();
        }

        public IKeyValueEnumerator<Bytes, byte[]> ReverseFetch(Bytes fromKey, Bytes toKey, long from, long to)
        {
            throw new NotImplementedException();
        }

        public IKeyValueEnumerator<Bytes, byte[]> ReverseFetchAll(long from, long to)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}