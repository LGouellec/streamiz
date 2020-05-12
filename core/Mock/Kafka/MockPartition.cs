using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockPartition
    {
        private readonly List<(byte[], byte[])> log = new List<(byte[], byte[])>();

        public MockPartition(int indice)
        {
            Size = 0;
        }

        public int Size { get; private set; } = 0;
        public long LowOffset { get; private set; } = 0;
        public long HighOffset { get; private set; } = 0;

        internal void AddMessageInLog(byte[] key, byte[] value)
        {
            log.Add((key, value));
            ++Size;
            ++HighOffset;
        }

        internal TestRecord<byte[], byte[]> GetMessage(long offset) =>
            offset <= Size - 1 ? new TestRecord<byte[], byte[]> { Key = log[(int)offset].Item1, Value = log[(int)offset].Item2 } : null;
    }
}
