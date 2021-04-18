using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Enumerator;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal interface ISegmentedBytesStore : IStateStore
    {
        IKeyValueEnumerator<Bytes, byte[]> Fetch(Bytes key, long from, long to);
        IKeyValueEnumerator<Bytes, byte[]> ReverseFetch(Bytes key, long from, long to);
        IKeyValueEnumerator<Bytes, byte[]> Fetch(Bytes fromKey, Bytes toKey, long from, long to);
        IKeyValueEnumerator<Bytes, byte[]> ReverseFetch(Bytes fromKey, Bytes toKey, long from, long to);
        IKeyValueEnumerator<Bytes, byte[]> All();
        IKeyValueEnumerator<Bytes, byte[]> ReverseAll();
        IKeyValueEnumerator<Bytes, byte[]> FetchAll(long from, long to);
        IKeyValueEnumerator<Bytes, byte[]> ReverseFetchAll(long from, long to);
        void Remove(Bytes key);
        void Put(Bytes key, byte[] value);
        byte[] Get(Bytes key);
    }
}