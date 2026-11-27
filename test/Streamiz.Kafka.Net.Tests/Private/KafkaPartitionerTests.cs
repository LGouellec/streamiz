using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class KafkaPartitionerTests
    {
        #region Reference vectors

        [Test]
        public void Murmur2MatchesJavaProducer()
        {
            // test vectors of org.apache.kafka.common.utils.UtilsTest#testMurmur2
            var cases = new Dictionary<byte[], int>
            {
                { Encoding.UTF8.GetBytes("21"), -973932308 },
                { Encoding.UTF8.GetBytes("foobar"), -790332482 },
                { Encoding.UTF8.GetBytes("a-little-bit-long-string"), -985981536 },
                { Encoding.UTF8.GetBytes("a-little-bit-longer-string"), -1486304829 },
                { Encoding.UTF8.GetBytes("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"), -58897971 },
                { new[] { (byte)'a', (byte)'b', (byte)'c' }, 479470107 }
            };

            foreach (var c in cases)
                Assert.AreEqual(c.Value, KafkaPartitioner.Murmur2(c.Key), Encoding.UTF8.GetString(c.Key));
        }

        [Test]
        public void Crc32ReferenceValues()
        {
            Assert.AreEqual(0xCBF43926u, KafkaPartitioner.Crc32(Encoding.ASCII.GetBytes("123456789")));
            Assert.AreEqual(0u, KafkaPartitioner.Crc32(Array.Empty<byte>()));
            Assert.AreEqual(0u, KafkaPartitioner.Crc32(null));
        }

        #endregion

        #region Random partitioning rules

        [Test]
        public void RandomPartitionersOnlyForNullOrEmptyKeys()
        {
            var empty = Array.Empty<byte>();

            Assert.AreEqual(Partition.Any, KafkaPartitioner.PartitionFor(Encoding.UTF8.GetBytes("key"), 4, Partitioner.Random));

            // consistent_random: null and empty keys are random
            Assert.AreEqual(Partition.Any, KafkaPartitioner.PartitionFor(null, 4, Partitioner.ConsistentRandom));
            Assert.AreEqual(Partition.Any, KafkaPartitioner.PartitionFor(empty, 4, Partitioner.ConsistentRandom));
            // librdkafka default is consistent_random
            Assert.AreEqual(Partition.Any, KafkaPartitioner.PartitionFor(null, 4, null));

            // murmur2_random: only null keys are random
            Assert.AreEqual(Partition.Any, KafkaPartitioner.PartitionFor(null, 4, Partitioner.Murmur2Random));
            Assert.AreNotEqual(Partition.Any, KafkaPartitioner.PartitionFor(empty, 4, Partitioner.Murmur2Random));

            // non random partitioners map null keys to a single partition
            Assert.AreEqual(new Partition(0), KafkaPartitioner.PartitionFor(null, 4, Partitioner.Consistent));
            Assert.AreNotEqual(Partition.Any, KafkaPartitioner.PartitionFor(null, 4, Partitioner.Murmur2));

            for (int i = 0; i < 20; ++i)
            {
                var p = KafkaPartitioner.ChoosePartition(null, 3, Partitioner.Murmur2Random);
                Assert.That(p, Is.InRange(0, 2));
            }

            Assert.Throws<ArgumentOutOfRangeException>(() => KafkaPartitioner.PartitionFor(empty, 0, Partitioner.Murmur2));
        }

        #endregion

        #region Cross-check with librdkafka

        private delegate int NativePartitioner(IntPtr rkt, byte[] key, UIntPtr keylen, int partitionCnt, IntPtr rktOpaque, IntPtr msgOpaque);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        private static extern int rd_kafka_msg_partitioner_consistent(IntPtr rkt, byte[] key, UIntPtr keylen, int partitionCnt, IntPtr rktOpaque, IntPtr msgOpaque);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        private static extern int rd_kafka_msg_partitioner_consistent_random(IntPtr rkt, byte[] key, UIntPtr keylen, int partitionCnt, IntPtr rktOpaque, IntPtr msgOpaque);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        private static extern int rd_kafka_msg_partitioner_murmur2(IntPtr rkt, byte[] key, UIntPtr keylen, int partitionCnt, IntPtr rktOpaque, IntPtr msgOpaque);

        [DllImport("librdkafka", CallingConvention = CallingConvention.Cdecl)]
        private static extern int rd_kafka_msg_partitioner_murmur2_random(IntPtr rkt, byte[] key, UIntPtr keylen, int partitionCnt, IntPtr rktOpaque, IntPtr msgOpaque);



        private static IEnumerable<byte[]> Keys()
        {
            var random = new Random(42);
            yield return Array.Empty<byte>();
            yield return Encoding.UTF8.GetBytes("key1");
            yield return Encoding.UTF8.GetBytes("foobar");
            for (int i = 0; i < 300; ++i)
            {
                var key = new byte[random.Next(1, 40)];
                random.NextBytes(key);
                yield return key;
            }
        }

        [Test]
        public void MatchesLibrdkafkaPartitioners()
        {
            try
            {
                Library.Load();
                rd_kafka_msg_partitioner_murmur2(IntPtr.Zero, new byte[] { 1 }, (UIntPtr)1, 1, IntPtr.Zero, IntPtr.Zero);
            }
            catch (Exception e) when (e is DllNotFoundException or EntryPointNotFoundException)
            {
                Assert.Ignore($"librdkafka native library not available: {e.Message}");
            }

            var partitioners = new List<(Partitioner partitioner, NativePartitioner native, bool randomOnEmpty)>
            {
                (Partitioner.Consistent, rd_kafka_msg_partitioner_consistent, false),
                (Partitioner.ConsistentRandom, rd_kafka_msg_partitioner_consistent_random, true),
                (Partitioner.Murmur2, rd_kafka_msg_partitioner_murmur2, false),
                (Partitioner.Murmur2Random, rd_kafka_msg_partitioner_murmur2_random, false),
            };

            int checks = 0;
            foreach (var (partitioner, native, randomOnEmpty) in partitioners)
            {
                foreach (var count in new[] { 1, 2, 3, 6, 7, 12, 100 })
                {
                    foreach (var key in Keys())
                    {
                        // a random partition needs a real topic handle in librdkafka: not comparable
                        if (key.Length == 0 && randomOnEmpty)
                            continue;

                        var expected = native(IntPtr.Zero, key, (UIntPtr)key.Length, count, IntPtr.Zero, IntPtr.Zero);
                        var actual = KafkaPartitioner.PartitionFor(key, count, partitioner);
                        Assert.AreEqual(expected, actual.Value, $"{partitioner}, {count} partitions, key {BitConverter.ToString(key)}");
                        ++checks;
                    }
                }
            }

            Assert.Greater(checks, 4 * 7 * 300);
        }

        #endregion
    }
}
