using System;
using System.Collections.Generic;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.TestDriver
{
    /// <summary>
    /// Tests for the AdvanceWallClockTime functionality which allows testing
    /// PROCESSING_TIME punctuations in the TopologyTestDriver.
    /// </summary>
    public class AdvanceWallClockTimeTests
    {
        private class ProcessingTimePunctuationProcessor : IProcessor<string, string>
        {
            public static List<long> PunctuationTimes { get; } = new();
            public static int PunctuationIntervalMs { get; set; } = 100;

            public void Init(ProcessorContext<string, string> context)
            {
                context.Schedule(
                    TimeSpan.FromMilliseconds(PunctuationIntervalMs),
                    PunctuationType.PROCESSING_TIME,
                    timestamp => PunctuationTimes.Add(timestamp));
            }

            public void Process(Record<string, string> record)
            {
            }

            public void Close()
            {
            }
        }

        [SetUp]
        public void Setup()
        {
            ProcessingTimePunctuationProcessor.PunctuationTimes.Clear();
            ProcessingTimePunctuationProcessor.PunctuationIntervalMs = 100;
        }

        [Test]
        public void Should_Trigger_Processing_Time_Punctuation_After_Advancing_Time()
        {
            // Arrange
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-advance-wall-clock-time"
            };

            ProcessingTimePunctuationProcessor.PunctuationIntervalMs = 100;

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .Process(new ProcessorBuilder<string, string>()
                    .Processor<ProcessingTimePunctuationProcessor>()
                    .Build());

            var topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("input");

                // Act - pipe a record to initialize the processor and schedule the punctuation
                inputTopic.PipeInput("key1", "value1");

                // Assert - no punctuation yet (time hasn't advanced enough)
                Assert.AreEqual(0, ProcessingTimePunctuationProcessor.PunctuationTimes.Count,
                    "No punctuation should have fired yet");

                // Act - advance time past the punctuation interval
                driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(110));

                // Assert - punctuation should have fired
                Assert.AreEqual(1, ProcessingTimePunctuationProcessor.PunctuationTimes.Count,
                    "Punctuation should have fired once after advancing time");

                // Act - advance time again
                driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(100));

                // Assert - another punctuation
                Assert.AreEqual(2, ProcessingTimePunctuationProcessor.PunctuationTimes.Count,
                    "Punctuation should have fired twice");
            }
        }

        [Test]
        public void Should_Not_Trigger_Punctuation_Before_Interval_Elapsed()
        {
            // Arrange
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-no-early-punctuation"
            };

            ProcessingTimePunctuationProcessor.PunctuationIntervalMs = 500;

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .Process(new ProcessorBuilder<string, string>()
                    .Processor<ProcessingTimePunctuationProcessor>()
                    .Build());

            var topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("input");

                // Initialize the processor
                inputTopic.PipeInput("key", "value");

                // Advance time, but not past the interval
                driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(400));

                // Assert - no punctuation should have fired
                Assert.AreEqual(0, ProcessingTimePunctuationProcessor.PunctuationTimes.Count,
                    "Punctuation should not fire before interval elapsed");
            }
        }

        [Test]
        public void Should_Trigger_Multiple_Punctuations_For_Multiple_Intervals()
        {
            // Arrange
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-multiple-punctuations"
            };

            ProcessingTimePunctuationProcessor.PunctuationIntervalMs = 100;

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .Process(new ProcessorBuilder<string, string>()
                    .Processor<ProcessingTimePunctuationProcessor>()
                    .Build());

            var topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("input");
                inputTopic.PipeInput("key", "value");

                // Advance time by 5 intervals worth
                for (int i = 0; i < 5; i++)
                {
                    driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(110));
                }

                // Assert - should have multiple punctuations
                Assert.AreEqual(5, ProcessingTimePunctuationProcessor.PunctuationTimes.Count,
                    "Should have 5 punctuations for 5 time advances");
            }
        }

        [Test]
        public void Should_Throw_NotSupportedException_For_ClusterInMemory_Mode()
        {
            // Arrange
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-cluster-mode"
            };

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .To("output");

            var topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY))
            {
                // Act & Assert
                Assert.Throws<NotSupportedException>(() =>
                    driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(100)));
            }
        }

        [Test]
        public void Should_Use_Mock_Time_For_Scheduling_New_Punctuations()
        {
            // This test verifies that after advancing time, new punctuations
            // scheduled use the mock time, not real wall clock time.

            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-mock-time-scheduling"
            };

            ProcessingTimePunctuationProcessor.PunctuationIntervalMs = 200;

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .Process(new ProcessorBuilder<string, string>()
                    .Processor<ProcessingTimePunctuationProcessor>()
                    .Build());

            var topology = builder.Build();

            using (var driver = new TopologyTestDriver(topology, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("input");
                inputTopic.PipeInput("key", "value");

                // Advance time and capture punctuation times
                driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(210));
                Assert.AreEqual(1, ProcessingTimePunctuationProcessor.PunctuationTimes.Count);
                var firstPunctuationTime = ProcessingTimePunctuationProcessor.PunctuationTimes[0];

                driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(210));
                Assert.AreEqual(2, ProcessingTimePunctuationProcessor.PunctuationTimes.Count);
                var secondPunctuationTime = ProcessingTimePunctuationProcessor.PunctuationTimes[1];

                // The difference between punctuation times should be approximately
                // the interval (within the time we advanced)
                var timeDifference = secondPunctuationTime - firstPunctuationTime;
                Assert.GreaterOrEqual(timeDifference, 200,
                    "Time difference between punctuations should be at least the interval");
            }
        }
    }
}