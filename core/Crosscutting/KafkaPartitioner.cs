using System;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Crosscutting
{
    /// <summary>
    /// Computes the partition chosen by the Confluent.Kafka (librdkafka) producer for a key,
    /// for each <see cref="Partitioner"/> supported by the producer.
    /// Used by the mock clients of the <see cref="Mock.TopologyTestDriver"/>, so that tests route records like a real producer.
    /// </summary>
    internal static class KafkaPartitioner
    {
        /// <summary>
        /// librdkafka default partitioner, used when <see cref="ProducerConfig.Partitioner"/> is not set.
        /// </summary>
        internal const Partitioner LibrdkafkaDefault = Partitioner.ConsistentRandom;

        private static readonly uint[] Crc32Table = BuildCrc32Table();

        /// <summary>
        /// Partition of a key, or <see cref="Partition.Any"/> when the partitioner picks a random partition
        /// (null key with a "random" partitioner, or the <see cref="Partitioner.Random"/> partitioner).
        /// </summary>
        /// <param name="key">serialized key (may be null)</param>
        /// <param name="partitionCount">number of partitions of the topic</param>
        /// <param name="partitioner">producer partitioner, librdkafka default when null</param>
        /// <returns>the partition, or <see cref="Partition.Any"/></returns>
        internal static Partition PartitionFor(byte[] key, int partitionCount, Partitioner? partitioner)
        {
            if (partitionCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(partitionCount), partitionCount, "The number of partitions must be positive");

            switch (partitioner ?? LibrdkafkaDefault)
            {
                case Partitioner.Random:
                    return Partition.Any;
                case Partitioner.Consistent:
                    return (int)(Crc32(key) % (uint)partitionCount);
                case Partitioner.ConsistentRandom:
                    // empty and null keys are randomly partitioned
                    return key == null || key.Length == 0 ? Partition.Any : (int)(Crc32(key) % (uint)partitionCount);
                case Partitioner.Murmur2:
                    return (Murmur2(key) & 0x7fffffff) % partitionCount;
                case Partitioner.Murmur2Random:
                    // only null keys are randomly partitioned
                    return key == null ? Partition.Any : (Murmur2(key) & 0x7fffffff) % partitionCount;
                default:
                    throw new NotSupportedException($"Partitioner {partitioner} is not supported");
            }
        }

        /// <summary>
        /// Partition of a key, picking a random partition when the partitioner does.
        /// </summary>
        internal static int ChoosePartition(byte[] key, int partitionCount, Partitioner? partitioner)
        {
            var partition = PartitionFor(key, partitionCount, partitioner);
            return partition == Partition.Any
                ? RandomGenerator.GetInt32(partitionCount)
                : partition.Value;
        }

        /// <summary>
        /// Murmur2 hash, compatible with the Java producer (seed 0x9747b28c).
        /// </summary>
        internal static int Murmur2(byte[] data)
        {
            data ??= Array.Empty<byte>();
            int length = data.Length;
            const int seed = unchecked((int)0x9747b28c);
            const int m = 0x5bd1e995;
            const int r = 24;

            unchecked
            {
                int h = seed ^ length;
                int length4 = length / 4;

                for (int i = 0; i < length4; i++)
                {
                    int i4 = i * 4;
                    int k = (data[i4] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) +
                            ((data[i4 + 3] & 0xff) << 24);
                    k *= m;
                    k ^= (int)((uint)k >> r);
                    k *= m;
                    h *= m;
                    h ^= k;
                }

                int index = length4 * 4;
                switch (length % 4)
                {
                    case 3:
                        h ^= (data[index + 2] & 0xff) << 16;
                        h ^= (data[index + 1] & 0xff) << 8;
                        h ^= data[index] & 0xff;
                        h *= m;
                        break;
                    case 2:
                        h ^= (data[index + 1] & 0xff) << 8;
                        h ^= data[index] & 0xff;
                        h *= m;
                        break;
                    case 1:
                        h ^= data[index] & 0xff;
                        h *= m;
                        break;
                }

                h ^= (int)((uint)h >> 13);
                h *= m;
                h ^= (int)((uint)h >> 15);
                return h;
            }
        }

        /// <summary>
        /// CRC-32 (IEEE 802.3) checksum, as used by the librdkafka <c>consistent</c> partitioners.
        /// </summary>
        internal static uint Crc32(byte[] data)
        {
            uint crc = 0xFFFFFFFF;
            if (data != null)
            {
                foreach (var b in data)
                    crc = Crc32Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
            }
            return ~crc;
        }

        private static uint[] BuildCrc32Table()
        {
            var table = new uint[256];
            for (uint i = 0; i < 256; i++)
            {
                uint c = i;
                for (int k = 0; k < 8; k++)
                    c = (c & 1) != 0 ? 0xEDB88320 ^ (c >> 1) : c >> 1;
                table[i] = c;
            }
            return table;
        }
    }
}
