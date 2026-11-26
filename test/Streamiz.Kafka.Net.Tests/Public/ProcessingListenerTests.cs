using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class ProcessingListenerTests
    {
        // Ambient scope set by the listener, like Activity.Current would be with a tracing listener
        private static readonly AsyncLocal<string> CurrentScope = new();

        private class Event
        {
            public string Kind { get; set; }
            public string Topic { get; set; }
            public long Offset { get; set; }
            public string ProcessorName { get; set; }
            public PunctuationType PunctuationType { get; set; }
            public long Timestamp { get; set; }
            public Exception Exception { get; set; }
            public object State { get; set; }
        }

        private class RecordingListener : IProcessingListener
        {
            private int counter;
            public List<Event> Events { get; } = new();

            public object OnProcessStart(ProcessorContext context, ConsumeResult<byte[], byte[]> record)
            {
                Assert.AreEqual(record.Topic, context.Topic);
                Assert.AreEqual(record.Offset.Value, context.Offset);
                Assert.AreEqual(record.Partition.Value, context.Partition.Value);

                var previous = CurrentScope.Value;
                var scope = $"process-{++counter}-{record.Topic}-{record.Offset.Value}";
                CurrentScope.Value = scope;
                Events.Add(new Event {Kind = "process-start", Topic = record.Topic, Offset = record.Offset.Value, State = scope});
                return (scope, previous);
            }

            public void OnProcessEnd(object state, Exception exception)
            {
                var (scope, previous) = ((string, string)) state;
                Assert.AreEqual(scope, CurrentScope.Value);
                Events.Add(new Event {Kind = "process-end", Exception = exception, State = scope});
                CurrentScope.Value = previous;
            }

            public object OnPunctuateStart(ProcessorContext context, string processorName, PunctuationType punctuationType, long timestamp)
            {
                var previous = CurrentScope.Value;
                var scope = $"punctuate-{++counter}-{processorName}";
                CurrentScope.Value = scope;
                Events.Add(new Event
                {
                    Kind = "punctuate-start", ProcessorName = processorName, PunctuationType = punctuationType,
                    Timestamp = timestamp, State = scope
                });
                return (scope, previous);
            }

            public void OnPunctuateEnd(object state, Exception exception)
            {
                var (scope, previous) = ((string, string)) state;
                Events.Add(new Event {Kind = "punctuate-end", Exception = exception, State = scope});
                CurrentScope.Value = previous;
            }

            public List<Event> Starts(string kind = "process-start") => Events.Where(e => e.Kind == kind).ToList();
        }

        private class ThrowingListener : IProcessingListener
        {
            public int Calls;

            public object OnProcessStart(ProcessorContext context, ConsumeResult<byte[], byte[]> record)
            {
                ++Calls;
                throw new InvalidOperationException("listener start");
            }

            public void OnProcessEnd(object state, Exception exception)
            {
                ++Calls;
                throw new InvalidOperationException("listener end");
            }

            public object OnPunctuateStart(ProcessorContext context, string processorName, PunctuationType punctuationType, long timestamp)
            {
                ++Calls;
                throw new InvalidOperationException("listener punctuate start");
            }

            public void OnPunctuateEnd(object state, Exception exception)
            {
                ++Calls;
                throw new InvalidOperationException("listener punctuate end");
            }
        }

        private class ThrowingEndListener : IProcessingListener
        {
            public Exception EndException;

            public object OnProcessStart(ProcessorContext context, ConsumeResult<byte[], byte[]> record) => null;

            public void OnProcessEnd(object state, Exception exception)
            {
                EndException = exception;
                throw new InvalidOperationException("listener end");
            }

            public object OnPunctuateStart(ProcessorContext context, string processorName, PunctuationType punctuationType, long timestamp) => null;

            public void OnPunctuateEnd(object state, Exception exception)
                => throw new InvalidOperationException("listener punctuate end");
        }

        // SerDes capturing the ambient scope during (de)serialization
        private class ScopeCapturingSerDes : StringSerDes
        {
            public List<string> DeserializeScopes { get; } = new();
            public List<string> SerializeScopes { get; } = new();

            public override string Deserialize(byte[] data, SerializationContext context)
            {
                DeserializeScopes.Add(CurrentScope.Value);
                return base.Deserialize(data, context);
            }

            public override byte[] Serialize(string data, SerializationContext context)
            {
                SerializeScopes.Add(CurrentScope.Value);
                return base.Serialize(data, context);
            }
        }

        private class PunctuationProcessor : IProcessor<string, string>
        {
            public static readonly List<string> Scopes = new();

            public void Init(ProcessorContext<string, string> context)
            {
                context.Schedule(TimeSpan.FromMilliseconds(10), PunctuationType.STREAM_TIME,
                    _ => Scopes.Add(CurrentScope.Value));
                context.Schedule(TimeSpan.FromMilliseconds(100), PunctuationType.PROCESSING_TIME,
                    _ => Scopes.Add(CurrentScope.Value));
            }

            public void Process(Record<string, string> record) { }

            public void Close() { }
        }

        private class FailingPunctuationProcessor : IProcessor<string, string>
        {
            public static Exception Thrown;
            private bool thrown;

            public void Init(ProcessorContext<string, string> context)
            {
                context.Schedule(TimeSpan.FromMilliseconds(100), PunctuationType.PROCESSING_TIME,
                    _ =>
                    {
                        if (thrown) return;
                        thrown = true;
                        throw Thrown;
                    });
            }

            public void Process(Record<string, string> record) { }

            public void Close() { }
        }

        [SetUp]
        public void Setup()
        {
            CurrentScope.Value = null;
            PunctuationProcessor.Scopes.Clear();
        }

        private static StreamConfig<StringSerDes, StringSerDes> BuildConfig(IProcessingListener listener)
            => new()
            {
                ApplicationId = $"test-processing-listener-{Guid.NewGuid()}",
                ProcessingListener = listener
            };

        [Test]
        public void DefaultProcessingListenerIsNull()
        {
            var config = new StreamConfig();
            Assert.IsNull(config.ProcessingListener);
        }

        [Test]
        public void CloneKeepProcessingListener()
        {
            var listener = new RecordingListener();
            var config = new StreamConfig {ProcessingListener = listener};
            Assert.AreSame(listener, config.Clone().ProcessingListener);
        }

        [Test]
        [TestCase(TopologyTestDriver.Mode.SYNC_TASK)]
        [TestCase(TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY)]
        public void ListenerWrapsWholeSubTopologyForEachRecord(TopologyTestDriver.Mode mode)
        {
            var listener = new RecordingListener();
            var sourceSerdes = new ScopeCapturingSerDes();
            var sinkSerdes = new ScopeCapturingSerDes();
            var processorScopes = new List<string>();

            var builder = new StreamBuilder();
            builder.Stream("input", new StringSerDes(), sourceSerdes)
                .MapValues((v, _) =>
                {
                    processorScopes.Add(CurrentScope.Value);
                    return v.ToUpper();
                })
                .To("output", new StringSerDes(), sinkSerdes);

            using (var driver = new TopologyTestDriver(builder.Build(), BuildConfig(listener), mode))
            {
                var input = driver.CreateInputTopic<string, string>("input");
                var output = driver.CreateOutputTopic<string, string>("output", TimeSpan.FromSeconds(5));
                input.PipeInput("key1", "value1");
                input.PipeInput("key2", "value2");
                var results = output.ReadKeyValueList().ToList();
                Assert.AreEqual(2, results.Count);
            }

            Assert.AreEqual(4, listener.Events.Count);
            Assert.AreEqual(new[] {"process-start", "process-end", "process-start", "process-end"},
                listener.Events.Select(e => e.Kind).ToArray());

            var starts = listener.Starts();
            Assert.AreEqual("input", starts[0].Topic);
            Assert.AreEqual(0, starts[0].Offset);
            Assert.AreEqual("input", starts[1].Topic);
            Assert.AreEqual(1, starts[1].Offset);
            Assert.IsTrue(listener.Events.Where(e => e.Kind == "process-end").All(e => e.Exception == null));

            var expectedScopes = starts.Select(s => (string) s.State).ToList();
            // deserialization by the source processor, processors and sink serialization all run inside the listener scope.
            // The record is also deserialized once when it is buffered (timestamp extraction), outside of any scope.
            Assert.AreEqual(4, sourceSerdes.DeserializeScopes.Count);
            CollectionAssert.AreEqual(expectedScopes, sourceSerdes.DeserializeScopes.Where(s => s != null).ToList());
            CollectionAssert.AreEqual(expectedScopes, processorScopes);
            CollectionAssert.AreEqual(expectedScopes, sinkSerdes.SerializeScopes);

            // the scope is restored after each record
            if (mode == TopologyTestDriver.Mode.SYNC_TASK)
                Assert.IsNull(CurrentScope.Value);
        }

        [Test]
        public void ListenerObservesRepartitionSources()
        {
            var listener = new RecordingListener();
            var afterRepartitionScopes = new List<string>();

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .SelectKey((k, v, _) => v)
                .Repartition(Repartitioned<string, string>.As("repart"))
                .MapValues((v, _) =>
                {
                    afterRepartitionScopes.Add(CurrentScope.Value);
                    return v;
                })
                .To("output");

            var topology = builder.Build();
            var describeWithoutListener = topology.Describe().ToString();

            using (var driver = new TopologyTestDriver(topology, BuildConfig(listener)))
            {
                var input = driver.CreateInputTopic<string, string>("input");
                var output = driver.CreateOutputTopic<string, string>("output");
                input.PipeInput("key1", "value1");
                Assert.AreEqual(1, output.ReadKeyValueList().Count());
            }

            // the topology is not modified
            Assert.AreEqual(describeWithoutListener, topology.Describe().ToString());

            var starts = listener.Starts();
            Assert.AreEqual(2, starts.Count);
            Assert.AreEqual("input", starts[0].Topic);
            StringAssert.Contains("repart-repartition", starts[1].Topic);
            CollectionAssert.AreEqual(new[] {starts[1].State}, afterRepartitionScopes);
        }

        [Test]
        public void ProcessingExceptionIsPassedToListenerAndRethrown()
        {
            var listener = new RecordingListener();
            var thrown = new InvalidOperationException("processing failure");

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValues<string>((_, _) => throw thrown)
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), BuildConfig(listener));
            var input = driver.CreateInputTopic<string, string>("input");
            var caught = Assert.Catch(() => input.PipeInput("key1", "value1"));

            var end = listener.Events.Single(e => e.Kind == "process-end");
            Assert.IsNotNull(end.Exception);
            Assert.AreSame(caught, end.Exception);
            Assert.IsTrue(ReferenceEquals(thrown, caught) || ReferenceEquals(thrown, caught.InnerException));
            Assert.IsNull(CurrentScope.Value);
        }

        [Test]
        public void ListenerExceptionsAreIgnored()
        {
            var listener = new ThrowingListener();

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValues((v, _) => v.ToUpper())
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), BuildConfig(listener));
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");
            input.PipeInput("key1", "value1");
            input.PipeInput("key2", "value2");

            var results = output.ReadKeyValueList().ToList();
            Assert.AreEqual(2, results.Count);
            Assert.AreEqual("VALUE1", results[0].Message.Value);
            // start throws, so end is never invoked
            Assert.AreEqual(2, listener.Calls);
        }

        [Test]
        public void ListenerExceptionDoesNotHideProcessingException()
        {
            var listener = new ThrowingEndListener();
            var thrown = new InvalidOperationException("processing failure");

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValues<string>((_, _) => throw thrown)
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), BuildConfig(listener));
            var input = driver.CreateInputTopic<string, string>("input");
            var caught = Assert.Catch(() => input.PipeInput("key1", "value1"));

            Assert.AreNotEqual("listener end", caught.Message);
            Assert.AreSame(caught, listener.EndException);
        }

        [Test]
        public void ListenerWrapsPunctuations()
        {
            var listener = new RecordingListener();

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .Process(new ProcessorBuilder<string, string>()
                    .Processor<PunctuationProcessor>()
                    .Build(), named: "my-processor");

            using (var driver = new TopologyTestDriver(builder.Build(), BuildConfig(listener)))
            {
                var input = driver.CreateInputTopic<string, string>("input");
                var now = DateTime.Now;
                input.PipeInput("key1", "value1", now);
                input.PipeInput("key2", "value2", now.AddMilliseconds(50));
                driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(150));
            }

            var punctuations = listener.Starts("punctuate-start");
            Assert.IsTrue(punctuations.Any(p => p.PunctuationType == PunctuationType.STREAM_TIME));
            Assert.IsTrue(punctuations.Any(p => p.PunctuationType == PunctuationType.PROCESSING_TIME));
            Assert.IsTrue(punctuations.All(p => p.ProcessorName == "my-processor"));
            Assert.IsTrue(punctuations.All(p => p.Timestamp > 0));

            Assert.AreEqual(punctuations.Count, listener.Events.Count(e => e.Kind == "punctuate-end"));
            CollectionAssert.AreEqual(punctuations.Select(p => (string) p.State).ToList(), PunctuationProcessor.Scopes);
            Assert.IsNull(CurrentScope.Value);
        }

        [Test]
        public void PunctuationExceptionIsPassedToListener()
        {
            var listener = new RecordingListener();
            FailingPunctuationProcessor.Thrown = new InvalidOperationException("punctuation failure");

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .Process(new ProcessorBuilder<string, string>()
                    .Processor<FailingPunctuationProcessor>()
                    .Build());

            using var driver = new TopologyTestDriver(builder.Build(), BuildConfig(listener));
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("key1", "value1");
            var caught = Assert.Catch(() => driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(150)));

            var end = listener.Events.First(e => e.Kind == "punctuate-end");
            Assert.AreSame(FailingPunctuationProcessor.Thrown, end.Exception);
            Assert.IsTrue(ReferenceEquals(FailingPunctuationProcessor.Thrown, caught) ||
                          ReferenceEquals(FailingPunctuationProcessor.Thrown, caught.InnerException));
        }

        [Test]
        public void ListenerObservesGlobalTableSource()
        {
            var listener = new RecordingListener();

            var builder = new StreamBuilder();
            builder.GlobalTable("global", InMemory.As<string, string>("global-store"));
            builder.Stream<string, string>("input").To("output");

            using (var driver = new TopologyTestDriver(builder.Build(), BuildConfig(listener)))
            {
                var global = driver.CreateInputTopic<string, string>("global");
                global.PipeInput("key1", "value1");
                var store = driver.GetKeyValueStore<string, string>("global-store");
                Assert.AreEqual("value1", store.Get("key1"));
            }

            var starts = listener.Starts();
            Assert.AreEqual(1, starts.Count);
            Assert.AreEqual("global", starts[0].Topic);
            Assert.AreEqual(2, listener.Events.Count);
        }

        [Test]
        public void ListenerObservesAsyncProcessorTopics()
        {
            var listener = new RecordingListener();
            var asyncScopes = new List<string>();

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValuesAsync(async (record, _) =>
                {
                    asyncScopes.Add(CurrentScope.Value);
                    return await Task.FromResult(record.Value.ToUpper());
                })
                .To("output");

            using (var driver = new TopologyTestDriver(builder.Build(), BuildConfig(listener)))
            {
                var input = driver.CreateInputTopic<string, string>("input");
                var output = driver.CreateOutputTopic<string, string>("output");
                input.PipeInput("key1", "value1");
                Assert.AreEqual("VALUE1", output.ReadKeyValue().Message.Value);
            }

            var topics = listener.Starts().Select(s => s.Topic).ToList();
            Assert.AreEqual(3, topics.Count);
            Assert.AreEqual("input", topics[0]);
            Assert.IsTrue(topics.Any(t => t.EndsWith("-request")), string.Join(",", topics));
            Assert.IsTrue(topics.Any(t => t.EndsWith("-response")), string.Join(",", topics));

            var requestStart = listener.Starts().Single(s => s.Topic.EndsWith("-request"));
            CollectionAssert.AreEqual(new[] {requestStart.State}, asyncScopes);
        }
    }
}
