using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Streamiz.Kafka.Net.Metrics.OpenTelemetry;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Tracing;

namespace Streamiz.Kafka.Net.Tests.Public
{
    [NonParallelizable]
    public class StreamizTracingTests
    {
        private const string TraceId = "0af7651916cd43dd8448eb211c80319c";
        private const string ParentSpanId = "b7ad6b7169203331";
        private const string TraceParent = "00-" + TraceId + "-" + ParentSpanId + "-01";

        private ActivityListener listener;
        private ConcurrentQueue<Activity> activities;
        private string inputTopic;
        private string outputTopic;

        [SetUp]
        public void Setup()
        {
            var suffix = Guid.NewGuid().ToString("N");
            inputTopic = "input-" + suffix;
            outputTopic = "output-" + suffix;
            activities = new ConcurrentQueue<Activity>();
        }

        [TearDown]
        public void TearDown()
        {
            listener?.Dispose();
            listener = null;
        }

        private void StartListener()
        {
            listener = new ActivityListener
            {
                ShouldListenTo = s => s.Name == StreamizInstrumentation.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = a => activities.Enqueue(a)
            };
            ActivitySource.AddActivityListener(listener);
        }

        // Activities of this test only (the source is static and shared by every test running in this process)
        private List<Activity> Captured(string topicContains)
            => activities
                .Where(a => a.GetTagItem("messaging.destination.name") is string t && t.Contains(topicContains))
                .ToList();

        private static string HeaderValue(Headers headers, string key)
            => headers != null && headers.TryGetLastBytes(key, out var bytes) ? Encoding.UTF8.GetString(bytes) : null;

        private static TopologyTestDriver BuildDriver(Action<StreamBuilder> build, string applicationId = "test-tracing")
        {
            var config = new StreamConfig<StringSerDes, StringSerDes> { ApplicationId = applicationId };
            var builder = new StreamBuilder();
            build(builder);
            return new TopologyTestDriver(builder.Build(), config);
        }

        [Test]
        public void NoListenerNoActivityAndHeadersUnchanged()
        {
            Assert.IsFalse(StreamizActivitySource.Source.HasListeners(),
                "another listener is subscribed to the Streamiz source");

            using var driver = BuildDriver(b => b.Stream<string, string>(inputTopic).MapValues((v, _) => v.ToUpper()).To(outputTopic));
            var input = driver.CreateInputTopic<string, string>(inputTopic);
            var output = driver.CreateOutputTopic<string, string>(outputTopic);

            var headers = new Headers();
            headers.Add(StreamizInstrumentation.TraceParentHeader, Encoding.UTF8.GetBytes(TraceParent));
            headers.Add("custom", Encoding.UTF8.GetBytes("value"));
            input.PipeInput("key", "value", headers);

            var record = output.ReadKeyValue();
            Assert.AreEqual("VALUE", record.Message.Value);
            // original trace context is forwarded untouched, exactly like before
            Assert.AreEqual(TraceParent, HeaderValue(record.Message.Headers, StreamizInstrumentation.TraceParentHeader));
            Assert.AreEqual("value", HeaderValue(record.Message.Headers, "custom"));
            Assert.IsNull(Activity.Current);
        }

        [Test]
        public void ProcessSpanIsChildOfIncomingTraceParentAndSendSpanIsInjected()
        {
            StartListener();

            using var driver = BuildDriver(b => b.Stream<string, string>(inputTopic).MapValues((v, _) => v.ToUpper()).To(outputTopic), "app-tracing");
            var input = driver.CreateInputTopic<string, string>(inputTopic);
            var output = driver.CreateOutputTopic<string, string>(outputTopic);

            var headers = new Headers();
            headers.Add(StreamizInstrumentation.TraceParentHeader, Encoding.UTF8.GetBytes(TraceParent));
            headers.Add(StreamizInstrumentation.TraceStateHeader, Encoding.UTF8.GetBytes("vendor=value"));
            headers.Add("custom", Encoding.UTF8.GetBytes("value"));
            input.PipeInput("key", "value", headers);

            var record = output.ReadKeyValue();
            Assert.AreEqual("VALUE", record.Message.Value);

            var process = Captured(inputTopic).Single();
            var send = Captured(outputTopic).Single();

            // process span
            Assert.AreEqual($"process {inputTopic}", process.DisplayName);
            Assert.AreEqual(ActivityKind.Consumer, process.Kind);
            Assert.AreEqual(TraceId, process.TraceId.ToHexString());
            Assert.AreEqual(ParentSpanId, process.ParentSpanId.ToHexString());
            Assert.AreEqual("vendor=value", process.TraceStateString);
            Assert.AreEqual("kafka", process.GetTagItem("messaging.system"));
            Assert.AreEqual("process", process.GetTagItem("messaging.operation.name"));
            Assert.AreEqual("process", process.GetTagItem("messaging.operation.type"));
            Assert.AreEqual("0", process.GetTagItem("messaging.destination.partition.id"));
            Assert.AreEqual(0L, process.GetTagItem("messaging.kafka.offset"));
            Assert.AreEqual("app-tracing", process.GetTagItem("messaging.consumer.group.name"));
            Assert.AreEqual("0-0", process.GetTagItem("streamiz.task.id"));
            Assert.AreEqual(ActivityStatusCode.Unset, process.Status);

            // send span
            Assert.AreEqual($"send {outputTopic}", send.DisplayName);
            Assert.AreEqual(ActivityKind.Producer, send.Kind);
            Assert.AreEqual(TraceId, send.TraceId.ToHexString());
            Assert.AreEqual(process.SpanId, send.ParentSpanId);
            Assert.AreEqual("send", send.GetTagItem("messaging.operation.name"));
            Assert.AreEqual("kafka", send.GetTagItem("messaging.system"));

            // propagation to the next hop
            Assert.AreEqual(send.Id, HeaderValue(record.Message.Headers, StreamizInstrumentation.TraceParentHeader));
            Assert.AreEqual("vendor=value", HeaderValue(record.Message.Headers, StreamizInstrumentation.TraceStateHeader));
            Assert.AreEqual("value", HeaderValue(record.Message.Headers, "custom"));
            Assert.AreEqual(1, record.Message.Headers.Count(h => h.Key == StreamizInstrumentation.TraceParentHeader));

            // consumed record headers are not mutated
            Assert.AreEqual(TraceParent, HeaderValue(headers, StreamizInstrumentation.TraceParentHeader));
            Assert.IsNull(Activity.Current);
        }

        [Test]
        public void ProcessSpanIsRootWithoutIncomingContext()
        {
            StartListener();

            using var driver = BuildDriver(b => b.Stream<string, string>(inputTopic).To(outputTopic));
            var input = driver.CreateInputTopic<string, string>(inputTopic);
            var output = driver.CreateOutputTopic<string, string>(outputTopic);

            input.PipeInput("key", "value");
            var record = output.ReadKeyValue();

            var process = Captured(inputTopic).Single();
            var send = Captured(outputTopic).Single();

            Assert.AreEqual(default(ActivitySpanId), process.ParentSpanId);
            Assert.AreEqual(process.TraceId, send.TraceId);
            Assert.AreEqual(process.SpanId, send.ParentSpanId);
            Assert.AreEqual(send.Id, HeaderValue(record.Message.Headers, StreamizInstrumentation.TraceParentHeader));
        }

        [Test]
        public void OneSendSpanPerSinkSharingTheSameProcessSpan()
        {
            StartListener();
            var outputTopic2 = outputTopic + "-bis";

            using var driver = BuildDriver(b =>
            {
                var stream = b.Stream<string, string>(inputTopic);
                stream.To(outputTopic);
                stream.To(outputTopic2);
            });
            var input = driver.CreateInputTopic<string, string>(inputTopic);
            var output = driver.CreateOutputTopic<string, string>(outputTopic);
            var output2 = driver.CreateOutputTopic<string, string>(outputTopic2);

            input.PipeInput("key", "value");
            var record1 = output.ReadKeyValue();
            var record2 = output2.ReadKeyValue();

            var process = Captured(inputTopic).Single();
            var sends = activities.Where(a => a.Kind == ActivityKind.Producer && a.ParentSpanId == process.SpanId).ToList();
            Assert.AreEqual(2, sends.Count);

            var traceParents = new[]
            {
                HeaderValue(record1.Message.Headers, StreamizInstrumentation.TraceParentHeader),
                HeaderValue(record2.Message.Headers, StreamizInstrumentation.TraceParentHeader)
            };
            CollectionAssert.AreEquivalent(sends.Select(s => s.Id), traceParents);
        }

        [Test]
        public void TraceIsPropagatedThroughRepartitionTopic()
        {
            StartListener();

            using var driver = BuildDriver(b => b
                .Stream<string, string>(inputTopic)
                .SelectKey((k, v, _) => v)
                .Repartition(Repartitioned<string, string>.As("tracing-repartition"))
                .To(outputTopic));
            var input = driver.CreateInputTopic<string, string>(inputTopic);
            var output = driver.CreateOutputTopic<string, string>(outputTopic);

            var headers = new Headers();
            headers.Add(StreamizInstrumentation.TraceParentHeader, Encoding.UTF8.GetBytes(TraceParent));
            input.PipeInput("key", "value", headers);
            var record = output.ReadKeyValue();
            Assert.IsNotNull(record);

            var all = activities.Where(a => a.TraceId.ToHexString() == TraceId).ToList();
            var processInput = all.Single(a => a.DisplayName == $"process {inputTopic}");
            var sendRepartition = all.Single(a => a.Kind == ActivityKind.Producer && a.DisplayName.Contains("tracing-repartition"));
            var processRepartition = all.Single(a => a.Kind == ActivityKind.Consumer && a.DisplayName.Contains("tracing-repartition"));
            var sendOutput = all.Single(a => a.DisplayName == $"send {outputTopic}");

            Assert.AreEqual(ParentSpanId, processInput.ParentSpanId.ToHexString());
            Assert.AreEqual(processInput.SpanId, sendRepartition.ParentSpanId);
            Assert.AreEqual(sendRepartition.SpanId, processRepartition.ParentSpanId);
            Assert.AreEqual(processRepartition.SpanId, sendOutput.ParentSpanId);
            Assert.AreEqual(sendOutput.Id, HeaderValue(record.Message.Headers, StreamizInstrumentation.TraceParentHeader));
        }

        [Test]
        public void ProcessSpanRecordsException()
        {
            StartListener();

            using var driver = BuildDriver(b => b
                .Stream<string, string>(inputTopic)
                .MapValues<string>((v, _) => throw new InvalidOperationException("boom"))
                .To(outputTopic));
            var input = driver.CreateInputTopic<string, string>(inputTopic);

            Assert.That(() => input.PipeInput("key", "value"), Throws.Exception);

            var process = Captured(inputTopic).Single();
            Assert.AreEqual(ActivityStatusCode.Error, process.Status);
            Assert.IsNotNull(process.GetTagItem("error.type"));
            Assert.IsTrue(process.Events.Any(e => e.Name == "exception"));
            Assert.IsEmpty(Captured(outputTopic));
            Assert.IsNull(Activity.Current);
        }

        [Test]
        public void TombstoneIsTagged()
        {
            StartListener();

            using var driver = BuildDriver(b => b.Stream<string, string>(inputTopic).To(outputTopic));
            var input = driver.CreateInputTopic<string, string>(inputTopic);
            var output = driver.CreateOutputTopic<string, string>(outputTopic);

            input.PipeInput("key", null);
            output.ReadKeyValue();

            Assert.That(Captured(inputTopic).Single().GetTagItem("messaging.kafka.message.tombstone"), Is.True);
            Assert.That(Captured(outputTopic).Single().GetTagItem("messaging.kafka.message.tombstone"), Is.True);
        }

        [Test]
        public void InvalidTraceParentIsIgnored()
        {
            StartListener();

            using var driver = BuildDriver(b => b.Stream<string, string>(inputTopic).To(outputTopic));
            var input = driver.CreateInputTopic<string, string>(inputTopic);
            var output = driver.CreateOutputTopic<string, string>(outputTopic);

            var headers = new Headers();
            headers.Add(StreamizInstrumentation.TraceParentHeader, Encoding.UTF8.GetBytes("not-a-trace-parent"));
            input.PipeInput("key", "value", headers);
            var record = output.ReadKeyValue();

            var process = Captured(inputTopic).Single();
            Assert.AreEqual(default(ActivitySpanId), process.ParentSpanId);
            Assert.AreEqual(Captured(outputTopic).Single().Id,
                HeaderValue(record.Message.Headers, StreamizInstrumentation.TraceParentHeader));
        }

        private class CollectingProcessor : BaseProcessor<Activity>
        {
            public readonly ConcurrentQueue<Activity> Ended = new();
            public override void OnEnd(Activity data) => Ended.Enqueue(data);
        }

        [Test]
        public void OpenTelemetryTracerProviderExtension()
        {
            var processor = new CollectingProcessor();
            using (Sdk.CreateTracerProviderBuilder()
                       .AddStreamizInstrumentation()
                       .AddProcessor(processor)
                       .Build())
            {
                using var driver = BuildDriver(b => b.Stream<string, string>(inputTopic).To(outputTopic));
                var input = driver.CreateInputTopic<string, string>(inputTopic);
                var output = driver.CreateOutputTopic<string, string>(outputTopic);

                var headers = new Headers();
                headers.Add(StreamizInstrumentation.TraceParentHeader, Encoding.UTF8.GetBytes(TraceParent));
                input.PipeInput("key", "value", headers);
                var record = output.ReadKeyValue();

                var ours = processor.Ended
                    .Where(a => a.GetTagItem("messaging.destination.name") is string t &&
                                (t == inputTopic || t == outputTopic))
                    .ToList();
                Assert.AreEqual(2, ours.Count);
                Assert.IsTrue(ours.All(a => a.TraceId.ToHexString() == TraceId));
                Assert.AreEqual(ours.Single(a => a.Kind == ActivityKind.Producer).Id,
                    HeaderValue(record.Message.Headers, StreamizInstrumentation.TraceParentHeader));
            }
        }
    }
}
