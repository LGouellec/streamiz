using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Private
{
    /// <summary>
    /// Tests for per-processor ParallelProcessingConfig API.
    ///
    /// IMPORTANT: These tests verify the API works correctly, but cannot test actual parallel processing
    /// behavior because TopologyTestDriver does not use ExternalStreamThread where ProcessingStrategy
    /// parallelism is implemented.
    ///
    /// After the refactoring (REFACTORING_SUMMARY.md 2026-04-10):
    /// - ParallelProcessingConfig at the processor level creates a unique request topic
    /// - Each request topic gets its own ProcessingStrategy in ExternalStreamThread
    /// - TopologyTestDriver processes records synchronously without ExternalStreamThread
    /// - Real parallel processing requires integration tests with actual Kafka cluster
    /// </summary>
    public class PerProcessorParallelTests
    {
        [Test]
        public void MapValuesAsync_WithParallelConfig_AcceptsConfigAndProcessesRecords()
        {
            // This test verifies the API accepts ParallelProcessingConfig without errors
            // NOTE: TopologyTestDriver cannot test actual parallel execution (no ExternalStreamThread)
            var processedKeys = new ConcurrentBag<string>();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-parallel-mapvalues";

            var builder = new StreamBuilder();

            builder.Stream<string, string>("input-topic")
                .MapValuesAsync(
                    async (record, ctx) =>
                    {
                        processedKeys.Add(record.Key);
                        await Task.Delay(10); // Simulate async work
                        return record.Value.ToUpper();
                    },
                    retryPolicy: RetryPolicy.NewBuilder().Build(),
                    parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 4))
                .To("output-topic");

            Topology topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("input-topic");
                var outputTopic = driver.CreateOutputTopic<string, string>("output-topic");

                // Send multiple messages
                for (int i = 0; i < 10; i++)
                {
                    inputTopic.PipeInput($"key-{i}", $"value-{i}");
                }

                Thread.Sleep(500);

                // Verify all records were processed
                Assert.AreEqual(10, processedKeys.Count, "All records should be processed");
            }
        }

        [Test]
        public void MapAsync_WithSequentialConfig_ProcessesInOrder()
        {
            var processingOrder = new List<string>();
            var lockObj = new object();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-sequential-map";

            var builder = new StreamBuilder();

            builder.Stream<string, string>("input-topic")
                .MapAsync(
                    async (record, ctx) =>
                    {
                        lock (lockObj)
                        {
                            processingOrder.Add(record.Key);
                        }

                        await Task.Delay(10);

                        return new KeyValuePair<string, string>(record.Key, record.Value);
                    },
                    retryPolicy: RetryPolicy.NewBuilder().Build(),
                    parallelProcessingConfig: null) // null means sequential
                .To("output-topic");

            Topology topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("input-topic");

                for (int i = 0; i < 5; i++)
                {
                    inputTopic.PipeInput($"key-{i}", $"value-{i}");
                }

                Thread.Sleep(500);

                // Sequential processing should maintain order
                Assert.AreEqual(5, processingOrder.Count);
                for (int i = 0; i < 5; i++)
                {
                    Assert.AreEqual($"key-{i}", processingOrder[i]);
                }
            }
        }

        [Test]
        public void FlatMapValuesAsync_WithParallelConfig_AcceptsConfigAndProcessesRecords()
        {
            // This test verifies the API accepts ParallelProcessingConfig without errors
            // NOTE: TopologyTestDriver cannot test actual parallel execution (no ExternalStreamThread)
            var processedCount = 0;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-parallel-flatmapvalues";

            var builder = new StreamBuilder();

            builder.Stream<string, string>("input-topic")
                .FlatMapValuesAsync<string>(
                    async (record, ctx) =>
                    {
                        Interlocked.Increment(ref processedCount);
                        await Task.Delay(10);
                        return new[] { record.Value, record.Value.ToUpper() };
                    },
                    retryPolicy: RetryPolicy.NewBuilder().Build(),
                    parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 2))
                .To("output-topic");

            Topology topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("input-topic");

                // Send 10 messages
                for (int i = 0; i < 10; i++)
                {
                    inputTopic.PipeInput($"key-{i}", $"value-{i}");
                }

                Thread.Sleep(500);

                // Verify all records were processed
                Assert.AreEqual(10, processedCount, "All records should be processed");
            }
        }

        [Test]
        public void ForeachAsync_WithParallelConfig_AcceptsConfigAndProcessesRecords()
        {
            // This test verifies the API accepts ParallelProcessingConfig without errors
            // NOTE: TopologyTestDriver cannot test actual parallel execution (no ExternalStreamThread)
            var processedKeys = new ConcurrentBag<string>();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-parallel-foreach";

            var builder = new StreamBuilder();

            builder.Stream<string, string>("input-topic")
                .ForeachAsync(
                    async (record, ctx) =>
                    {
                        processedKeys.Add(record.Key);
                        await Task.Delay(10);
                    },
                    retryPolicy: RetryPolicy.NewBuilder().Build(),
                    parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 3));

            Topology topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("input-topic");

                for (int i = 0; i < 10; i++)
                {
                    inputTopic.PipeInput($"key-{i}", $"value-{i}");
                }

                Thread.Sleep(500);

                // Verify all records were processed
                Assert.AreEqual(10, processedKeys.Count, "All records should be processed");
            }
        }

        [Test]
        public void AsyncProcessing_WithRetryPolicy_RetriesOnFailure()
        {
            // This test verifies retry policy works correctly with async processors
            // NOTE: TopologyTestDriver cannot test actual parallel execution (no ExternalStreamThread)
            var attemptCounts = new ConcurrentDictionary<string, int>();
            var successKeys = new ConcurrentBag<string>();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-parallel-retry";

            var builder = new StreamBuilder();

            builder.Stream<string, string>("input-topic")
                .MapValuesAsync(
                    async (record, ctx) =>
                    {
                        var attempts = attemptCounts.AddOrUpdate(record.Key, 1, (k, v) => v + 1);

                        // Fail on first attempt, succeed on retry
                        if (attempts == 1)
                        {
                            await Task.Delay(10);
                            throw new InvalidOperationException("Simulated failure");
                        }

                        successKeys.Add(record.Key);
                        return record.Value.ToUpper();
                    },
                    retryPolicy: RetryPolicy.NewBuilder()
                        .NumberOfRetry(3)
                        .RetryBackOffMs(50)
                        .RetriableException<InvalidOperationException>()
                        .Build(),
                    parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 2))
                .To("output-topic");

            Topology topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("input-topic");

                inputTopic.PipeInput("key-1", "value-1");
                inputTopic.PipeInput("key-2", "value-2");

                Thread.Sleep(1000);

                // Both keys should have been retried and succeeded
                Assert.AreEqual(2, successKeys.Count, "Both records should succeed after retry");
                Assert.IsTrue(attemptCounts["key-1"] > 1, "key-1 should have been retried");
                Assert.IsTrue(attemptCounts["key-2"] > 1, "key-2 should have been retried");
            }
        }

        [Test]
        public void DifferentProcessors_CanHaveDifferentParallelConfigs()
        {
            // This test verifies that different processors can be configured with different
            // ParallelProcessingConfig settings without errors
            // NOTE: TopologyTestDriver cannot test actual parallel execution (no ExternalStreamThread)
            var processor1Count = 0;
            var processor2Count = 0;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-different-configs";

            var builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("input-topic");

            // First processor with concurrency 2
            stream.MapValuesAsync(
                async (record, ctx) =>
                {
                    Interlocked.Increment(ref processor1Count);
                    await Task.Delay(10);
                    return record.Value + "-p1";
                },
                parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 2))
                .To("output-topic-1");

            // Second processor with concurrency 4
            stream.MapValuesAsync(
                async (record, ctx) =>
                {
                    Interlocked.Increment(ref processor2Count);
                    await Task.Delay(10);
                    return record.Value + "-p2";
                },
                parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 4))
                .To("output-topic-2");

            Topology topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("input-topic");

                // Send test messages
                for (int i = 0; i < 5; i++)
                {
                    inputTopic.PipeInput($"key-{i}", $"value-{i}");
                }

                Thread.Sleep(500);

                // Both processors should have processed all records
                Assert.AreEqual(5, processor1Count, "Processor 1 should process all records");
                Assert.AreEqual(5, processor2Count, "Processor 2 should process all records");
            }
        }
    }
}
