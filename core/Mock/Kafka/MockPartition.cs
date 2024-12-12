using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockPartition
    {
        private readonly List<(byte[], byte[], long, Headers)> log = new();
        private readonly Dictionary<long, long> mappingOffsets = new();

        public MockPartition(int indice)
        {
            Size = 0;
            Index = indice;
        }

        public int Index { get; }
        public int Size { get; private set; } = 0;
        public long LowOffset { get; private set; } = Offset.Unset;
        public long HighOffset { get; private set; } = Offset.Unset;

        internal void AddMessageInLog(byte[] key, byte[] value, long timestamp, Headers headers)
        {
            mappingOffsets.Add(Size, log.Count);
            log.Add((key, value, timestamp, headers));
            ++Size;
            UpdateOffset();
        }

        private void UpdateOffset()
        {
            if (Size > 0)
            {
                LowOffset = mappingOffsets.Keys.Min();
                HighOffset = Size - 1;
            }
        }

        internal TestRecord<byte[], byte[]> GetMessage(long offset)
        {
            if (mappingOffsets.ContainsKey(offset))
            {
                var record = log[(int) mappingOffsets[offset]];
                return new TestRecord<byte[], byte[]>
                {
                    Key = record.Item1,
                    Value = record.Item2,
                    Timestamp = record.Item3.FromMilliseconds(),
                    Headers = record.Item4
                };
            }

            return null;
        }

        public void Remove(Offset tpoOffset)
        {
            var offsetsToRemove = mappingOffsets.Keys.Where(k => k < tpoOffset).OrderBy(g => g).ToList();
            foreach (var o in offsetsToRemove)
            {
                log.RemoveAt(0);
                mappingOffsets.Remove(o);
            }

            foreach (var kv in mappingOffsets)
                mappingOffsets.AddOrUpdate(kv.Key, kv.Value - offsetsToRemove.Count);

        }
    }
}
