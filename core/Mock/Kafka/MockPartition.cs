using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Mock.Kafka
{
    internal class MockPartition
    {
        private readonly int indice = 0;
        private readonly List<(byte[], byte[])> log = new List<(byte[], byte[])>();
        private int sizeLog;

        public MockPartition(int indice)
        {
            this.indice = indice;
            this.sizeLog = 0;
        }

        public int Size => sizeLog;

        internal void AddMessageInLog(byte[] key, byte[] value)
        {
            log.Add((key, value));
            ++sizeLog;
        }

        internal (byte[], byte[]) GetMessage(int offset) => log[offset];
    }
}
