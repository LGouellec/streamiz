using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Tests.Private
{
    /// <summary>
    /// Tests related to https://github.com/LGouellec/streamiz/issues/481 : internal timeouts, intervals and deadlines
    /// must rely on a monotonic clock, not on the local wall-clock time (DST change, NTP, manual adjustment).
    /// </summary>
    public class MonotonicClockTests
    {
        private class FakeMonotonicClock : IMonotonicClock
        {
            public long NowMs { get; private set; }

            public void Advance(TimeSpan ts) => NowMs += (long)ts.TotalMilliseconds;
        }

        private static Mock<IConsumer<byte[], byte[]>> CreateConsumer(
            FakeMonotonicClock clock,
            Func<int, ConsumeResult<byte[], byte[]>> resultFactory,
            List<TimeSpan> consumeTimeouts)
        {
            int call = 0;
            var consumer = new Mock<IConsumer<byte[], byte[]>>();
            consumer
                .Setup(c => c.Assignment)
                .Returns(new List<TopicPartition> { new("topic", 0) });
            consumer
                .Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns((TimeSpan ts) =>
                {
                    consumeTimeouts.Add(ts);
                    var result = resultFactory(call++);
                    // when no record is available, the consumer blocks during the whole timeout
                    clock.Advance(result == null ? ts : TimeSpan.FromMilliseconds(1));
                    return result;
                });
            return consumer;
        }

        private static ConsumeResult<byte[], byte[]> Record(int offset)
            => new()
            {
                TopicPartitionOffset = new TopicPartitionOffset("topic", 0, offset),
                Message = new Message<byte[], byte[]> { Key = new byte[] { 1 }, Value = new byte[] { 1 } }
            };

        [Test]
        public void MonotonicClockNeverGoesBack()
        {
            long previous = MonotonicClock.CurrentMs;
            for (int i = 0; i < 1000; ++i)
            {
                long current = MonotonicClock.Instance.NowMs;
                Assert.GreaterOrEqual(current, previous);
                previous = current;
            }

            long start = MonotonicClock.CurrentMs;
            Thread.Sleep(50);
            Assert.GreaterOrEqual(MonotonicClock.ElapsedSince(start), 40);
        }

        [Test]
        public void ConsumeRecordsWithoutRecordsReturnsAtTimeout()
        {
            var clock = new FakeMonotonicClock();
            var timeouts = new List<TimeSpan>();
            var consumer = CreateConsumer(clock, _ => null, timeouts);

            var records = consumer.Object.ConsumeRecords(TimeSpan.FromMilliseconds(100), 500, clock);

            Assert.IsEmpty(records);
            // the loop is driven by the monotonic clock only: it stops once the deadline is reached
            Assert.GreaterOrEqual(clock.NowMs, 100);
            Assert.Less(clock.NowMs, 100 + 1000);
            Assert.Less(timeouts.Count, 100);
        }

        [Test]
        public void ConsumeRecordsLowThroughputReturnsAtTimeout()
        {
            // Reproducer of #481 : a low-throughput stream (fewer records than MaxPollRecords)
            // must get its records back once the poll timeout is elapsed.
            var clock = new FakeMonotonicClock();
            var timeouts = new List<TimeSpan>();
            var consumer = CreateConsumer(clock, call => call < 3 ? Record(call) : null, timeouts);

            var records = consumer.Object.ConsumeRecords(TimeSpan.FromMilliseconds(100), 500, clock).ToList();

            Assert.AreEqual(3, records.Count);
            Assert.GreaterOrEqual(clock.NowMs, 100);
            Assert.Less(clock.NowMs, 100 + 1000);
        }

        [Test]
        public void ConsumeRecordsStopsWhenBatchIsFull()
        {
            var clock = new FakeMonotonicClock();
            var timeouts = new List<TimeSpan>();
            var consumer = CreateConsumer(clock, Record, timeouts);

            var records = consumer.Object.ConsumeRecords(TimeSpan.FromSeconds(10), 5, clock).ToList();

            Assert.AreEqual(5, records.Count);
            Assert.Less(clock.NowMs, 10000);
        }

        [Test]
        public void ConsumeRecordsWithSystemMonotonicClockReturnsAtTimeout()
        {
            var consumer = new Mock<IConsumer<byte[], byte[]>>();
            consumer
                .Setup(c => c.Assignment)
                .Returns(new List<TopicPartition> { new("topic", 0) });
            consumer
                .Setup(c => c.Consume(It.IsAny<TimeSpan>()))
                .Returns((TimeSpan ts) =>
                {
                    Thread.Sleep(ts);
                    return null;
                });

            var sw = Stopwatch.StartNew();
            var records = consumer.Object.ConsumeRecords(TimeSpan.FromMilliseconds(200), 500);
            sw.Stop();

            Assert.IsEmpty(records);
            Assert.GreaterOrEqual(sw.ElapsedMilliseconds, 150);
            Assert.Less(sw.ElapsedMilliseconds, 5000);
        }

        [Test]
        public void CoreDoesNotUseLocalWallClockTime()
        {
            // Guard against regressions: the local time (DateTime.Now) must not be used in the core library.
            // Use MonotonicClock for timeouts/intervals/durations and DateTime.UtcNow for epoch timestamps.
            var coreDirectory = FindCoreDirectory();
            if (coreDirectory == null)
                Assert.Ignore("Core sources not found, skipping the check");

            var regex = new Regex(@"\bDateTime\.Now\b");
            var offenders = Directory
                .EnumerateFiles(coreDirectory, "*.cs", SearchOption.AllDirectories)
                .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}")
                            && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}"))
                .SelectMany(f => File.ReadAllLines(f)
                    .Select((line, index) => (file: f, line, index)))
                .Where(l => !l.line.TrimStart().StartsWith("//") && regex.IsMatch(l.line))
                .Select(l => $"{Path.GetRelativePath(coreDirectory, l.file)}:{l.index + 1}: {l.line.Trim()}")
                .ToList();

            Assert.IsEmpty(offenders, string.Join(Environment.NewLine, offenders));
        }

        private static string FindCoreDirectory()
        {
            var directory = new DirectoryInfo(TestContext.CurrentContext.TestDirectory);
            while (directory != null)
            {
                var core = Path.Combine(directory.FullName, "core");
                if (File.Exists(Path.Combine(core, "Streamiz.Kafka.Net.csproj")))
                    return core;
                directory = directory.Parent;
            }

            return null;
        }
    }
}
