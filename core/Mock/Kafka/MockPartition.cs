using System.Collections.Generic;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockPartition
    {
        private readonly List<(byte[], byte[])> log = new List<(byte[], byte[])>();

        public MockPartition(int indice)
        {
            Size = 0;
            Index = indice;
        }

        public int Index { get; }
        public int Size { get; private set; } = 0;
        public long LowOffset { get; private set; } = Offset.Unset;
        public long HighOffset { get; private set; } = Offset.Unset;

        internal void AddMessageInLog(byte[] key, byte[] value)
        {
            log.Add((key, value));
            ++Size;
            UpdateOffset();
        }

        private void UpdateOffset()
        {
            if (Size > 0)
            {
                LowOffset = 0;
                HighOffset = Size - 1;
            }
        }

        internal TestRecord<byte[], byte[]> GetMessage(long offset)
            => offset <= Size - 1 ? new TestRecord<byte[], byte[]> { Key = log[(int)offset].Item1, Value = log[(int)offset].Item2 } : null;
    }
}
