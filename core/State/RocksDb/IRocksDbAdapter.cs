using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State
{
    internal interface IRocksDbAdapter
    {
        void Put(byte[] key, byte[] value);

        void PrepareBatch(IEnumerable<KeyValuePair<Bytes, byte[]>> entries,
                          WriteBatch batch);

        byte[] Get(byte[] key);

        byte[] GetOnly(byte[] key);

        IKeyValueEnumerator<Bytes, byte[]> Range(
            Bytes from,
            Bytes to, 
            bool forward);

        IKeyValueEnumerator<Bytes, byte[]> All(bool forward);

        long ApproximateNumEntries();

        void Flush();

        void PrepareBatchForRestore(
            IEnumerable<KeyValuePair<byte[], byte[]>> records,
            WriteBatch batch);

        void AddToBatch(
            byte[] key,
            byte[] value,
            WriteBatch batch);

        void Close();
    }
}
