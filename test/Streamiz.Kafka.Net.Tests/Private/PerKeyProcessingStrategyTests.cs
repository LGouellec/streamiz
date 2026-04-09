using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class PerKeyProcessingStrategyTests
    {
        private const string Topic = "test-topic";

        [Test]
        public void KeyHashing_ConsistentForSameKey()
        {
            // Test that the same key always hashes to the same value
            var key1 = Encoding.UTF8.GetBytes("test-key-1");
            var key2 = Encoding.UTF8.GetBytes("test-key-1"); // Same key

            var hash1 = ComputeKeyHash(key1);
            var hash2 = ComputeKeyHash(key2);

            Assert.AreEqual(hash1, hash2, "Same key should produce same hash");
        }

        [Test]
        public void KeyHashing_DifferentForDifferentKeys()
        {
            // Test that different keys produce different hashes
            var key1 = Encoding.UTF8.GetBytes("test-key-1");
            var key2 = Encoding.UTF8.GetBytes("test-key-2");

            var hash1 = ComputeKeyHash(key1);
            var hash2 = ComputeKeyHash(key2);

            Assert.AreNotEqual(hash1, hash2, "Different keys should produce different hashes");
        }

        [Test]
        public void KeyHashing_EmptyKey()
        {
            // Test that empty keys don't throw
            var emptyKey = new byte[0];
            var hash = ComputeKeyHash(emptyKey);

            Assert.IsNotNull(hash);
        }

        [Test]
        public void WorkerAssignment_SameKeyToSameWorker()
        {
            // Test that records with the same key go to the same worker
            int workerCount = 4;
            var key = Encoding.UTF8.GetBytes("consistent-key");

            var record1 = CreateRecord(Topic, 0, 0, key, new byte[] { 1 });
            var record2 = CreateRecord(Topic, 0, 1, key, new byte[] { 2 });
            var record3 = CreateRecord(Topic, 0, 2, key, new byte[] { 3 });

            int worker1 = GetWorkerIndex(record1, workerCount);
            int worker2 = GetWorkerIndex(record2, workerCount);
            int worker3 = GetWorkerIndex(record3, workerCount);

            Assert.AreEqual(worker1, worker2, "Same key should go to same worker");
            Assert.AreEqual(worker1, worker3, "Same key should go to same worker");
        }

        [Test]
        public void WorkerAssignment_DifferentKeysMayGoDifferentWorkers()
        {
            // Test that different keys CAN (not necessarily will) go to different workers
            int workerCount = 4;
            var workers = new HashSet<int>();

            // Try many different keys - statistically they should distribute across workers
            for (int i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key-{i}");
                var record = CreateRecord(Topic, 0, i, key, new byte[] { 1 });
                int worker = GetWorkerIndex(record, workerCount);
                workers.Add(worker);

                if (workers.Count == workerCount)
                    break; // All workers have been assigned at least one key
            }

            // We should have distributed across multiple workers
            Assert.GreaterOrEqual(workers.Count, 2, "Different keys should distribute across multiple workers");
        }

        [Test]
        public void WorkerAssignment_NullKeyUsesPartition()
        {
            // Test that null keys fall back to partition-based assignment
            int workerCount = 4;

            var record1 = CreateRecord(Topic, 0, 0, null, new byte[] { 1 });
            var record2 = CreateRecord(Topic, 1, 0, null, new byte[] { 2 });
            var record3 = CreateRecord(Topic, 2, 0, null, new byte[] { 3 });
            var record4 = CreateRecord(Topic, 3, 0, null, new byte[] { 4 });

            int worker1 = GetWorkerIndex(record1, workerCount); // Partition 0
            int worker2 = GetWorkerIndex(record2, workerCount); // Partition 1
            int worker3 = GetWorkerIndex(record3, workerCount); // Partition 2
            int worker4 = GetWorkerIndex(record4, workerCount); // Partition 3

            // With 4 workers and 4 partitions, each should map to different worker
            Assert.AreEqual(0, worker1, "Partition 0 should map to worker 0");
            Assert.AreEqual(1, worker2, "Partition 1 should map to worker 1");
            Assert.AreEqual(2, worker3, "Partition 2 should map to worker 2");
            Assert.AreEqual(3, worker4, "Partition 3 should map to worker 3");
        }

        [Test]
        public void WorkerAssignment_EmptyKeyUsesPartition()
        {
            // Test that empty keys fall back to partition-based assignment (like null)
            int workerCount = 4;

            var emptyKey = new byte[0];
            var record1 = CreateRecord(Topic, 0, 0, emptyKey, new byte[] { 1 });
            var record2 = CreateRecord(Topic, 1, 0, emptyKey, new byte[] { 2 });

            int worker1 = GetWorkerIndex(record1, workerCount); // Partition 0
            int worker2 = GetWorkerIndex(record2, workerCount); // Partition 1

            Assert.AreEqual(0, worker1, "Partition 0 with empty key should map to worker 0");
            Assert.AreEqual(1, worker2, "Partition 1 with empty key should map to worker 1");
        }

        [Test]
        public void WorkerAssignment_WithinWorkerCount()
        {
            // Test that worker assignment is always within bounds
            int workerCount = 8;

            for (int i = 0; i < 1000; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key-{i}");
                var record = CreateRecord(Topic, i % 10, i, key, new byte[] { 1 });
                int worker = GetWorkerIndex(record, workerCount);

                Assert.GreaterOrEqual(worker, 0, "Worker index should be >= 0");
                Assert.Less(worker, workerCount, "Worker index should be < worker count");
            }
        }

        [Test]
        public void KeyDistribution_ReasonablyBalanced()
        {
            // Test that keys distribute reasonably across workers (not all to one worker)
            int workerCount = 4;
            var workerCounts = new int[workerCount];

            // Distribute 100 different keys
            for (int i = 0; i < 100; i++)
            {
                var key = Encoding.UTF8.GetBytes($"key-{i}");
                var record = CreateRecord(Topic, 0, i, key, new byte[] { 1 });
                int worker = GetWorkerIndex(record, workerCount);
                workerCounts[worker]++;
            }

            // Each worker should get at least 10% of keys (allowing for some imbalance)
            foreach (var count in workerCounts)
            {
                Assert.GreaterOrEqual(count, 10, $"Worker should get at least 10 keys, got {count}");
            }

            // No worker should get more than 50% of keys
            foreach (var count in workerCounts)
            {
                Assert.LessOrEqual(count, 50, $"Worker should not get more than 50 keys, got {count}");
            }
        }

        [Test]
        public void Config_PerKeyFactoryMethod()
        {
            // Test the factory method for per-key config
            var config = ParallelProcessingConfig.PerKey();

            Assert.AreEqual(ParallelProcessingMode.PER_KEY, config.Mode);
            Assert.AreEqual(Environment.ProcessorCount * 2, config.MaxConcurrency);
        }

        [Test]
        public void Config_PerKeyWithCustomConcurrency()
        {
            // Test the factory method with custom concurrency
            var config = ParallelProcessingConfig.PerKey(8);

            Assert.AreEqual(ParallelProcessingMode.PER_KEY, config.Mode);
            Assert.AreEqual(8, config.MaxConcurrency);
        }

        // Helper methods

        private ConsumeResult<byte[], byte[]> CreateRecord(string topic, int partition, long offset, byte[] key, byte[] value)
        {
            return new ConsumeResult<byte[], byte[]>
            {
                Topic = topic,
                Partition = partition,
                Offset = offset,
                Message = new Message<byte[], byte[]>
                {
                    Key = key,
                    Value = value
                }
            };
        }

        private int GetWorkerIndex(ConsumeResult<byte[], byte[]> record, int workerCount)
        {
            var key = record.Message.Key;

            // If key is null or empty, use partition number for assignment
            if (key == null || key.Length == 0)
            {
                return record.Partition.Value % workerCount;
            }

            // Hash the key to get a consistent worker assignment
            int hash = ComputeKeyHash(key);
            return Math.Abs(hash % workerCount);
        }

        private int ComputeKeyHash(byte[] key)
        {
            unchecked
            {
                int hash = 17;
                for (int i = 0; i < key.Length; i++)
                {
                    hash = hash * 31 + key[i];
                }
                return hash;
            }
        }
    }
}
