using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class ProcessingListenerTests
    {
        private const string TopicTag = "messaging.destination.name";

        private sealed class TracingListener : IProcessingListener, IDisposable
        {
            private readonly ActivitySource source = new($"streamiz-tests-{Guid.NewGuid():N}");
            private readonly ActivityListener listener;

            public ConcurrentQueue<Activity> Stopped { get; } = new();
            public ConcurrentQueue<Exception> Exceptions { get; } = new();
            public ConcurrentQueue<(string ProcessorName, PunctuationType Type)> Punctuations { get; } = new();

            public TracingListener()
            {
                listener = new ActivityListener
                {
                    ShouldListenTo = s => s == source,
                    Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                    ActivityStopped = a => Stopped.Enqueue(a)
                };
                ActivitySource.AddActivityListener(listener);
            }

            public object OnProcessStart(ProcessorContext context, ConsumeResult<byte[], byte[]> record)
            {
                var activity = source.StartActivity($"{record.Topic} process", ActivityKind.Consumer);
                activity?.SetTag(TopicTag, context.Topic);
                activity?.SetTag("streamiz.task.id", context.Id.ToString());
                return activity;
            }

            public void OnProcessEnd(object state, Exception exception) => End(state, exception);

            public object OnPunctuateStart(ProcessorContext context, string processorName, PunctuationType punctuationType, long timestamp)
            {
                Punctuations.Enqueue((processorName, punctuationType));
                return source.StartActivity($"{processorName} punctuate");
            }

            public void OnPunctuateEnd(object state, Exception exception) => End(state, exception);

            private void End(object state, Exception exception)
            {
                if (exception != null)
                    Exceptions.Enqueue(exception);

                if (state is Activity activity)
                {
                    if (exception != null)
                        activity.SetStatus(ActivityStatusCode.Error, exception.Message);
                    activity.Dispose();
                }
            }

            public void Dispose()
            {
                listener.Dispose();
                source.Dispose();
            }
        }

        private sealed class ThrowingListener : IProcessingListener
        {
            public object OnProcessStart(ProcessorContext context, ConsumeResult<byte[], byte[]> record)
                => throw new InvalidOperationException("start");

            public void OnProcessEnd(object state, Exception exception)
                => throw new InvalidOperationException("end");

            public object OnPunctuateStart(ProcessorContext context, string processorName, PunctuationType punctuationType, long timestamp)
                => throw new InvalidOperationException("start");

            public void OnPunctuateEnd(object state, Exception exception)
                => throw new InvalidOperationException("end");
        }

        private sealed class ActivityCapturingSerDes : AbstractSerDes<string>
        {
            private readonly StringSerDes inner = new();
            public ConcurrentQueue<Activity> Seen { get; } = new();

            public override byte[] Serialize(string data, SerializationContext context)
            {
                Seen.Enqueue(Activity.Current);
                return inner.Serialize(data, context);
            }

            public override string Deserialize(byte[] data, SerializationContext context)
                => inner.Deserialize(data, context);
        }

        private class PunctuationProcessor : IProcessor<string, string>
        {
            public static ConcurrentQueue<Activity> Seen { get; } = new();

            public void Init(ProcessorContext<string, string> context)
                => context.Schedule(TimeSpan.FromMilliseconds(100), PunctuationType.PROCESSING_TIME,
                    _ => Seen.Enqueue(Activity.Current));

            public void Process(Record<string, string> record) { }

            public void Close() { }
        }

        private static StreamConfig<StringSerDes, StringSerDes> Config(IProcessingListener listener)
            => new()
            {
                ApplicationId = $"test-processing-listener-{Guid.NewGuid():N}",
                PollMs = 10,
                ProcessingListener = listener
            };

        [TestCase(TopologyTestDriver.Mode.SYNC_TASK)]
        [TestCase(TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY)]
        public void SpanCoversWholeForwardChainUpToTheSink(TopologyTestDriver.Mode mode)
        {
            using var listener = new TracingListener();
            var peeked = new ConcurrentQueue<Activity>();
            var sinkSerDes = new ActivityCapturingSerDes();

            var builder = new StreamBuilder();
            builder.Stream<string, string>("in")
                .Filter((_, _, _) => true)
                .MapValues((v, _) => v.ToUpperInvariant())
                .Peek((_, _, _) => peeked.Enqueue(Activity.Current))
                .To("out", new StringSerDes(), sinkSerDes);

            using (var driver = new TopologyTestDriver(builder.Build(), Config(listener), mode))
            {
                var input = driver.CreateInputTopic<string, string>("in");
                var output = driver.CreateOutputTopic<string, string>("out", TimeSpan.FromSeconds(10));
                input.PipeInput("k1", "v1");
                input.PipeInput("k2", "v2");
                Assert.AreEqual(2, output.ReadKeyValueList().Count());
                if (mode == TopologyTestDriver.Mode.SYNC_TASK)
                    Assert.IsNull(Activity.Current);
            }

            var spans = listener.Stopped.ToList();
            Assert.AreEqual(2, spans.Count);
            CollectionAssert.AreEqual(spans, peeked.ToList());
            CollectionAssert.AreEqual(spans, sinkSerDes.Seen.ToList());
            Assert.IsNull(spans[1].Parent, "Activity.Current must be restored after each record");
            Assert.IsTrue(spans.All(s => (string)s.GetTagItem(TopicTag) == "in"));
        }

        [TestCase(TopologyTestDriver.Mode.SYNC_TASK)]
        [TestCase(TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY)]
        public void RepartitionSourceGetsItsOwnSpan(TopologyTestDriver.Mode mode)
        {
            using var listener = new TracingListener();
            var peeked = new ConcurrentQueue<Activity>();
            var builder = new StreamBuilder();
            builder.Stream<string, string>("in")
                .SelectKey((_, v, _) => v)
                .GroupByKey()
                .Count()
                .ToStream()
                .Peek((_, _, _) => peeked.Enqueue(Activity.Current))
                .MapValues((v, _) => v.ToString())
                .To("out");

            using (var driver = new TopologyTestDriver(builder.Build(), Config(listener), mode))
            {
                var input = driver.CreateInputTopic<string, string>("in");
                var output = driver.CreateOutputTopic<string, string>("out", TimeSpan.FromSeconds(10));
                input.PipeInput("k1", "a");
                Assert.AreEqual("1", output.ReadKeyValue().Message.Value);
            }

            var spans = listener.Stopped.ToList();
            Assert.AreEqual(2, spans.Count);
            Assert.AreEqual("in", spans[0].GetTagItem(TopicTag));
            StringAssert.EndsWith("-repartition", (string)spans[1].GetTagItem(TopicTag));
            Assert.AreNotEqual(spans[0].GetTagItem("streamiz.task.id"), spans[1].GetTagItem("streamiz.task.id"));
            Assert.IsNull(spans[1].Parent);
            Assert.AreSame(spans[1], peeked.Single());
        }

        [Test]
        public void ProcessingExceptionIsPassedToEndAndRethrown()
        {
            using var listener = new TracingListener();
            var builder = new StreamBuilder();
            builder.Stream<string, string>("in")
                .MapValues((v, _) => v == "boom" ? throw new InvalidOperationException("boom") : v)
                .To("out");

            using (var driver = new TopologyTestDriver(builder.Build(), Config(listener)))
            {
                var input = driver.CreateInputTopic<string, string>("in");
                var thrown = Assert.Throws<InvalidOperationException>(() => input.PipeInput("k", "boom"));
                Assert.AreSame(thrown, listener.Exceptions.Single());
                Assert.IsNull(Activity.Current);
            }

            Assert.AreEqual(ActivityStatusCode.Error, listener.Stopped.Single().Status);
        }

        [Test]
        public void ListenerExceptionsAreIgnored()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("in")
                .MapValues((v, _) => v == "boom" ? throw new ArgumentException("boom") : v)
                .To("out");

            using var driver = new TopologyTestDriver(builder.Build(), Config(new ThrowingListener()));
            var input = driver.CreateInputTopic<string, string>("in");
            input.PipeInput("k", "v");
            Assert.AreEqual("v", driver.CreateOutputTopic<string, string>("out").ReadKeyValue().Message.Value);

            var thrown = Assert.Throws<ArgumentException>(() => input.PipeInput("k", "boom"));
            Assert.AreEqual("boom", thrown.Message);
        }

        [Test]
        public void GlobalTableRecordsAreObserved()
        {
            using var listener = new TracingListener();
            var builder = new StreamBuilder();
            builder.GlobalTable("global", InMemory.As<string, string>("global-store"));

            using (var driver = new TopologyTestDriver(builder.Build(), Config(listener)))
                driver.CreateInputTopic<string, string>("global").PipeInput("k", "v");

            Assert.AreEqual("global", listener.Stopped.Single().GetTagItem(TopicTag));
        }

        [Test]
        public void AsyncProcessorRequestAndResponseTopicsAreObserved()
        {
            using var listener = new TracingListener();
            var builder = new StreamBuilder();
            builder.Stream<string, string>("in")
                .MapValuesAsync(async (record, _) => await System.Threading.Tasks.Task.FromResult(record.Value.ToUpper()))
                .To("out");

            using (var driver = new TopologyTestDriver(builder.Build(), Config(listener)))
            {
                driver.CreateInputTopic<string, string>("in").PipeInput("k", "v");
                Assert.AreEqual("V", driver.CreateOutputTopic<string, string>("out").ReadKeyValue().Message.Value);
            }

            var topics = listener.Stopped.Select(s => (string)s.GetTagItem(TopicTag)).ToList();
            Assert.AreEqual(3, topics.Count, string.Join(", ", topics));
            Assert.AreEqual("in", topics[0]);
            StringAssert.EndsWith("-request", topics[1]);
            StringAssert.EndsWith("-response", topics[2]);
        }

        [Test]
        public void PunctuationIsObserved()
        {
            PunctuationProcessor.Seen.Clear();
            using var listener = new TracingListener();
            var builder = new StreamBuilder();
            builder.Stream<string, string>("in")
                .Process(new ProcessorBuilder<string, string>().Processor<PunctuationProcessor>().Build(), named: "punctuator");

            using (var driver = new TopologyTestDriver(builder.Build(), Config(listener)))
            {
                driver.CreateInputTopic<string, string>("in").PipeInput("k", "v");
                driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(110));
            }

            Assert.AreEqual(("punctuator", PunctuationType.PROCESSING_TIME), listener.Punctuations.Single());
            var spans = listener.Stopped.ToList();
            Assert.AreEqual(2, spans.Count);
            Assert.AreSame(spans[1], PunctuationProcessor.Seen.Single());
        }
    }
}
