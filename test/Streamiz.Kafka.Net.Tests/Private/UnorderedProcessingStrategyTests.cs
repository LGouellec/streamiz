using System;
using System.Collections.Generic;
using NUnit.Framework;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class UnorderedProcessingStrategyTests
    {
        [Test]
        public void Config_UnorderedFactoryMethod()
        {
            // Test the factory method for unordered config
            var config = ParallelProcessingConfig.Unordered();

            Assert.AreEqual(ParallelProcessingMode.UNORDERED, config.Mode);
            Assert.AreEqual(Environment.ProcessorCount * 4, config.MaxConcurrency);
        }

        [Test]
        public void Config_UnorderedWithCustomConcurrency()
        {
            // Test the factory method with custom concurrency
            var config = ParallelProcessingConfig.Unordered(16);

            Assert.AreEqual(ParallelProcessingMode.UNORDERED, config.Mode);
            Assert.AreEqual(16, config.MaxConcurrency);
        }

        [Test]
        public void Config_UnorderedValidation()
        {
            // Test config validation for unordered mode
            var config = new ParallelProcessingConfig
            {
                Mode = ParallelProcessingMode.UNORDERED,
                MaxConcurrency = 8,
                MaxQueuedRecords = 1000
            };

            Assert.DoesNotThrow(() => config.Validate());

            // Invalid concurrency
            config.MaxConcurrency = 0;
            Assert.Throws<ArgumentException>(() => config.Validate());

            config.MaxConcurrency = 8;

            // Invalid queue size
            config.MaxQueuedRecords = -1;
            Assert.Throws<ArgumentException>(() => config.Validate());
        }

        [Test]
        public void OrderingGuarantees_Comparison()
        {
            // Document the ordering guarantees for all strategies
            // This test serves as documentation

            // SEQUENTIAL: ✓ Partition Order, ✓ Key Order, ✓ Cross-Key Order
            var sequential = ParallelProcessingConfig.Sequential();
            Assert.AreEqual(ParallelProcessingMode.SEQUENTIAL, sequential.Mode);
            Assert.AreEqual(1, sequential.MaxConcurrency, "Sequential must have concurrency = 1");

            // PER_PARTITION: ✓ Partition Order, ✓ Key Order, ✗ Cross-Key Order
            var perPartition = ParallelProcessingConfig.PerPartition(4);
            Assert.AreEqual(ParallelProcessingMode.PER_PARTITION, perPartition.Mode);
            Assert.AreEqual(4, perPartition.MaxConcurrency);

            // PER_KEY: ✗ Partition Order, ✓ Key Order, ✗ Cross-Key Order
            var perKey = ParallelProcessingConfig.PerKey(8);
            Assert.AreEqual(ParallelProcessingMode.PER_KEY, perKey.Mode);
            Assert.AreEqual(8, perKey.MaxConcurrency);

            // UNORDERED: ✗ Partition Order, ✗ Key Order, ✗ Cross-Key Order (MAXIMUM PARALLELISM)
            var unordered = ParallelProcessingConfig.Unordered(16);
            Assert.AreEqual(ParallelProcessingMode.UNORDERED, unordered.Mode);
            Assert.AreEqual(16, unordered.MaxConcurrency);
        }

        [Test]
        public void Concurrency_UnorderedShouldBeHighest()
        {
            // Document that unordered has the highest default concurrency
            var sequential = ParallelProcessingConfig.Sequential();
            var perPartition = ParallelProcessingConfig.PerPartition();
            var perKey = ParallelProcessingConfig.PerKey();
            var unordered = ParallelProcessingConfig.Unordered();

            Assert.AreEqual(1, sequential.MaxConcurrency);
            Assert.AreEqual(Environment.ProcessorCount, perPartition.MaxConcurrency);
            Assert.AreEqual(Environment.ProcessorCount * 2, perKey.MaxConcurrency);
            Assert.AreEqual(Environment.ProcessorCount * 4, unordered.MaxConcurrency);

            // Unordered should have the highest default concurrency
            Assert.Greater(unordered.MaxConcurrency, perKey.MaxConcurrency);
            Assert.Greater(unordered.MaxConcurrency, perPartition.MaxConcurrency);
            Assert.Greater(unordered.MaxConcurrency, sequential.MaxConcurrency);
        }

        [Test]
        public void UseCases_Documentation()
        {
            // This test documents the recommended use cases for each strategy
            // It doesn't actually test functionality, but serves as documentation

            // SEQUENTIAL: When strict ordering is required or for backward compatibility
            // Use case: Processing financial transactions where order matters critically
            var sequential = ParallelProcessingConfig.Sequential();
            Assert.IsNotNull(sequential);

            // PER_PARTITION: When partition-level ordering matters but cross-partition doesn't
            // Use case: Processing logs from different servers (each server = partition)
            var perPartition = ParallelProcessingConfig.PerPartition();
            Assert.IsNotNull(perPartition);

            // PER_KEY: When key-level ordering matters (e.g., user sessions, entity updates)
            // Use case: Processing user events where each user's events must be ordered
            var perKey = ParallelProcessingConfig.PerKey();
            Assert.IsNotNull(perKey);

            // UNORDERED: When order doesn't matter and maximum throughput is needed
            // Use case: Processing independent analytics events, metrics, or logs
            var unordered = ParallelProcessingConfig.Unordered();
            Assert.IsNotNull(unordered);
        }

        [Test]
        public void PerformanceCharacteristics_Documentation()
        {
            // Document expected performance characteristics
            // This is documentation, not actual performance testing

            int processorCount = Environment.ProcessorCount;

            var sequential = ParallelProcessingConfig.Sequential();
            // Expected throughput: 1x (baseline)
            Assert.AreEqual(1, sequential.MaxConcurrency);

            var perPartition = ParallelProcessingConfig.PerPartition();
            // Expected throughput: 2-4x for I/O-bound workloads
            Assert.AreEqual(processorCount, perPartition.MaxConcurrency);

            var perKey = ParallelProcessingConfig.PerKey();
            // Expected throughput: 3-6x for I/O-bound workloads
            Assert.AreEqual(processorCount * 2, perKey.MaxConcurrency);

            var unordered = ParallelProcessingConfig.Unordered();
            // Expected throughput: 5-10x for I/O-bound workloads (maximum)
            Assert.AreEqual(processorCount * 4, unordered.MaxConcurrency);
        }

        [Test]
        public void AllStrategies_DefaultConfigsAreValid()
        {
            // Verify that all default factory method configs are valid
            var sequential = ParallelProcessingConfig.Sequential();
            Assert.DoesNotThrow(() => sequential.Validate());

            var perPartition = ParallelProcessingConfig.PerPartition();
            Assert.DoesNotThrow(() => perPartition.Validate());

            var perKey = ParallelProcessingConfig.PerKey();
            Assert.DoesNotThrow(() => perKey.Validate());

            var unordered = ParallelProcessingConfig.Unordered();
            Assert.DoesNotThrow(() => unordered.Validate());
        }

        [Test]
        public void AllStrategies_CustomConcurrencyConfigsAreValid()
        {
            // Verify that custom concurrency configs are valid
            var sequential = ParallelProcessingConfig.Sequential();
            Assert.DoesNotThrow(() => sequential.Validate());

            var perPartition = ParallelProcessingConfig.PerPartition(4);
            Assert.DoesNotThrow(() => perPartition.Validate());

            var perKey = ParallelProcessingConfig.PerKey(8);
            Assert.DoesNotThrow(() => perKey.Validate());

            var unordered = ParallelProcessingConfig.Unordered(16);
            Assert.DoesNotThrow(() => unordered.Validate());
        }

        [Test]
        public void ConfigValidation_AllModes()
        {
            // Test validation for all modes
            var modes = new[]
            {
                ParallelProcessingMode.SEQUENTIAL,
                ParallelProcessingMode.PER_PARTITION,
                ParallelProcessingMode.PER_KEY,
                ParallelProcessingMode.UNORDERED
            };

            foreach (var mode in modes)
            {
                var config = new ParallelProcessingConfig
                {
                    Mode = mode,
                    MaxConcurrency = mode == ParallelProcessingMode.SEQUENTIAL ? 1 : 4,
                    MaxQueuedRecords = 1000
                };

                Assert.DoesNotThrow(() => config.Validate(), $"Mode {mode} should validate successfully");
            }
        }
    }
}
