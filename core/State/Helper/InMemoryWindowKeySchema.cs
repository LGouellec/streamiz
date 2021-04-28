using System;

namespace Streamiz.Kafka.Net.State.Helper
{
    internal static class InMemoryWindowKeySchema
    {
        private static readonly int timestampSize = 8;

        internal static byte[] ExtractStoreKeyBytes(this byte[] data)
            => data.AsSpan(0, data.Length - timestampSize).ToArray();

        internal static long ExtractStoreTimestamp(this byte[] data)
            => BitConverter.ToInt64(data.AsSpan(data.Length - timestampSize, timestampSize));
    }
}
