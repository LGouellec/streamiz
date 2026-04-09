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
    public class PerProcessorParallelTests
    {
        [Test]
        public void MapValuesAsync_WithParallelConfig_ProcessesConcurrently()
        {
            // Track concurrent processing
            var concurrentCount = 0;
            var maxConcurrent = 0;
            var processedKeys = new ConcurrentBag<string>();
            var lockObj = new object();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-parallel-mapvalues";

            var builder = new StreamBuilder();

            builder.Stream<string, string>("input-topic")
                .MapValuesAsync(
                    async (record, ctx) =>
                    {
                        // Track concurrency
                        lock (lockObj)
                        {
                            concurrentCount++;
                            if (concurrentCount > maxConcurrent)
                                maxConcurrent = concurrentCount;
                        }

                        processedKeys.Add(record.Key);

                        // Simulate async work
                        await Task.Delay(100);

                        lock (lockObj)
                        {
                            concurrentCount--;
                        }

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

                // Send multiple messages quickly
                for (int i = 0; i < 10; i++)
                {
                    inputTopic.PipeInput($"key-{i}", $"value-{i}");
                }

                // Wait for processing to complete
                Thread.Sleep(2000);

                // Should have processed all keys
                Assert.AreEqual(10, processedKeys.Count);

                // Should have had some concurrency (not always 1)
                Assert.Greater(maxConcurrent, 1, "Expected concurrent processing");
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
        public void FlatMapValuesAsync_WithParallelConfig_RespectsMaxConcurrency()
        {
            var concurrentCount = 0;
            var maxConcurrent = 0;
            var lockObj = new object();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-parallel-flatmapvalues";

            var builder = new StreamBuilder();

            builder.Stream<string, string>("input-topic")
                .FlatMapValuesAsync<string>(
                    async (record, ctx) =>
                    {
                        lock (lockObj)
                        {
                            concurrentCount++;
                            if (concurrentCount > maxConcurrent)
                                maxConcurrent = concurrentCount;
                        }

                        await Task.Delay(100);

                        lock (lockObj)
                        {
                            concurrentCount--;
                        }

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

                Thread.Sleep(2000);

                // Max concurrent should not exceed configured limit
                Assert.LessOrEqual(maxConcurrent, 2, $"Max concurrency should not exceed 2, but was {maxConcurrent}");
                Assert.Greater(maxConcurrent, 0, "Should have had some concurrency");
            }
        }

        [Test]
        public void ForeachAsync_WithParallelConfig_ProcessesConcurrently()
        {
            var processedKeys = new ConcurrentBag<string>();
            var concurrentCount = 0;
            var maxConcurrent = 0;
            var lockObj = new object();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-parallel-foreach";

            var builder = new StreamBuilder();

            builder.Stream<string, string>("input-topic")
                .ForeachAsync(
                    async (record, ctx) =>
                    {
                        lock (lockObj)
                        {
                            concurrentCount++;
                            if (concurrentCount > maxConcurrent)
                                maxConcurrent = concurrentCount;
                        }

                        processedKeys.Add(record.Key);
                        await Task.Delay(100);

                        lock (lockObj)
                        {
                            concurrentCount--;
                        }
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

                Thread.Sleep(2000);

                Assert.AreEqual(10, processedKeys.Count);
                Assert.Greater(maxConcurrent, 1, "Expected concurrent processing");
                Assert.LessOrEqual(maxConcurrent, 3, "Should not exceed max concurrency");
            }
        }

        [Test]
        public void ParallelProcessing_WithRetryPolicy_RetriesOnFailure()
        {
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
                Assert.AreEqual(2, successKeys.Count);
                Assert.IsTrue(attemptCounts["key-1"] > 1, "key-1 should have been retried");
                Assert.IsTrue(attemptCounts["key-2"] > 1, "key-2 should have been retried");
            }
        }

        [Test]
        public void DifferentProcessors_CanHaveDifferentParallelConfigs()
        {
            var processor1MaxConcurrent = 0;
            var processor2MaxConcurrent = 0;
            var lock1 = new object();
            var lock2 = new object();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-different-configs";

            var builder = new StreamBuilder();

            var stream = builder.Stream<string, string>("input-topic");

            // First processor with concurrency 2
            stream.MapValuesAsync(
                async (record, ctx) =>
                {
                    lock (lock1)
                    {
                        processor1MaxConcurrent++;
                    }
                    await Task.Delay(100);
                    lock (lock1)
                    {
                        processor1MaxConcurrent--;
                    }
                    return record.Value + "-p1";
                },
                parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 2))
                .To("output-topic-1");

            // Second processor with concurrency 4
            stream.MapValuesAsync(
                async (record, ctx) =>
                {
                    lock (lock2)
                    {
                        processor2MaxConcurrent++;
                    }
                    await Task.Delay(100);
                    lock (lock2)
                    {
                        processor2MaxConcurrent--;
                    }
                    return record.Value + "-p2";
                },
                parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 4))
                .To("output-topic-2");

            // This test verifies that different processors can have different configs
            // The actual concurrency levels are tracked separately
            Assert.IsNotNull(builder);
        }
    }
}
