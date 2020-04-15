using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Mock.Kafka
{
    internal class MockPartition
    {
        private readonly int indice = 0;
        private readonly List<(byte[], byte[])> log = new List<(byte[], byte[])>();
        private int sizeLog = 0;

        public MockPartition(int indice)
        {
            this.indice = indice;
            this.sizeLog = 0;
        }

        public int Size => sizeLog;
        public long LowOffset { get; private set; } = 0;
        public long HighOffset { get; private set; } = 0;

        internal void AddMessageInLog(byte[] key, byte[] value)
        {
            log.Add((key, value));
            ++sizeLog;
            ++HighOffset;
        }

        internal TestRecord<byte[], byte[]> GetMessage(long offset) =>
            offset <= Size - 1 ? new TestRecord<byte[], byte[]> { Key = log[(int)offset].Item1, Value = log[(int)offset].Item2 } : null;
    }
}
