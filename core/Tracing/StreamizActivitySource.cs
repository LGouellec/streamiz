using System;
using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Tracing
{
    /// <summary>
    /// Internal helper creating the Streamiz spans, following the OpenTelemetry messaging semantic conventions.
    /// Every method is a no-op (and allocation free) when no listener is subscribed to <see cref="StreamizInstrumentation.ActivitySourceName"/>.
    /// </summary>
    internal static class StreamizActivitySource
    {
        internal const string MessagingSystem = "messaging.system";
        internal const string MessagingOperationName = "messaging.operation.name";
        internal const string MessagingOperationType = "messaging.operation.type";
        internal const string MessagingDestinationName = "messaging.destination.name";
        internal const string MessagingDestinationPartitionId = "messaging.destination.partition.id";
        internal const string MessagingConsumerGroupName = "messaging.consumer.group.name";
        internal const string MessagingKafkaOffset = "messaging.kafka.offset";
        internal const string MessagingKafkaTombstone = "messaging.kafka.message.tombstone";
        internal const string StreamizTaskId = "streamiz.task.id";

        internal const string ProcessOperation = "process";
        internal const string SendOperation = "send";

        internal static readonly ActivitySource Source = new(
            StreamizInstrumentation.ActivitySourceName,
            typeof(StreamizActivitySource).Assembly.GetName().Version?.ToString());

        /// <summary>
        /// Start a consumer "process" span for a consumed record, using the W3C context of its headers as parent.
        /// Returns null when tracing is disabled.
        /// </summary>
        internal static Activity StartProcess(ConsumeResult<byte[], byte[]> record, string applicationId, TaskId taskId)
        {
            if (!Source.HasListeners() || record == null)
                return null;

            ExtractContext(record.Message?.Headers, out var parentContext);

            var activity = Source.StartActivity(
                $"{ProcessOperation} {record.Topic}",
                ActivityKind.Consumer,
                parentContext);

            if (activity is { IsAllDataRequested: true })
            {
                activity.SetTag(MessagingSystem, "kafka");
                activity.SetTag(MessagingOperationName, ProcessOperation);
                activity.SetTag(MessagingOperationType, ProcessOperation);
                activity.SetTag(MessagingDestinationName, record.Topic);
                activity.SetTag(MessagingDestinationPartitionId, record.Partition.Value.ToString());
                activity.SetTag(MessagingKafkaOffset, record.Offset.Value);
                activity.SetTag(MessagingConsumerGroupName, applicationId);
                if (record.Message?.Value == null)
                    activity.SetTag(MessagingKafkaTombstone, true);
                if (taskId != null)
                    activity.SetTag(StreamizTaskId, taskId.ToString());
            }

            return activity;
        }

        /// <summary>
        /// Start a producer "send" span for a record produced to <paramref name="topic"/>.
        /// Returns null when tracing is disabled.
        /// </summary>
        internal static Activity StartSend(string topic, int? partition, bool tombstone, ProcessorContext context)
        {
            if (!Source.HasListeners())
                return null;

            var activity = Source.StartActivity($"{SendOperation} {topic}", ActivityKind.Producer);

            if (activity is { IsAllDataRequested: true })
            {
                activity.SetTag(MessagingSystem, "kafka");
                activity.SetTag(MessagingOperationName, SendOperation);
                activity.SetTag(MessagingOperationType, SendOperation);
                activity.SetTag(MessagingDestinationName, topic);
                if (partition.HasValue)
                    activity.SetTag(MessagingDestinationPartitionId, partition.Value.ToString());
                if (tombstone)
                    activity.SetTag(MessagingKafkaTombstone, true);
                var taskId = context?.Task?.Id;
                if (taskId != null)
                    activity.SetTag(StreamizTaskId, taskId.ToString());
            }

            return activity;
        }

        /// <summary>
        /// Mark the activity as failed, following the OpenTelemetry exception semantic conventions.
        /// </summary>
        internal static void SetException(Activity activity, Exception exception)
        {
            if (activity == null || exception == null)
                return;

            activity.SetStatus(ActivityStatusCode.Error, exception.Message);
            activity.SetTag("error.type", exception.GetType().FullName);

            if (activity.IsAllDataRequested)
            {
                activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
                {
                    { "exception.type", exception.GetType().FullName },
                    { "exception.message", exception.Message },
                    { "exception.stacktrace", exception.ToString() }
                }));
            }
        }

        /// <summary>
        /// Extract the W3C trace context from the headers.
        /// </summary>
        internal static bool ExtractContext(Headers headers, out ActivityContext context)
        {
            context = default;
            if (headers == null || headers.Count == 0)
                return false;

            if (!headers.TryGetLastBytes(StreamizInstrumentation.TraceParentHeader, out var traceParentBytes) ||
                traceParentBytes == null)
                return false;

            string traceState = null;
            if (headers.TryGetLastBytes(StreamizInstrumentation.TraceStateHeader, out var traceStateBytes) &&
                traceStateBytes != null)
                traceState = Encoding.UTF8.GetString(traceStateBytes);

            return ActivityContext.TryParse(
                Encoding.UTF8.GetString(traceParentBytes),
                traceState,
                out context);
        }

        /// <summary>
        /// Return a copy of <paramref name="headers"/> where the W3C trace context headers are replaced by the context of <paramref name="activity"/>.
        /// The original headers are never mutated (they belong to the consumed record and can be shared between several sinks).
        /// If <paramref name="activity"/> is null or not W3C, the original headers are returned unchanged.
        /// </summary>
        internal static Headers InjectContext(Activity activity, Headers headers)
        {
            if (activity == null || activity.IdFormat != ActivityIdFormat.W3C || activity.Id == null)
                return headers;

            var newHeaders = new Headers();
            if (headers != null)
            {
                foreach (var header in headers)
                {
                    if (header.Key == StreamizInstrumentation.TraceParentHeader ||
                        header.Key == StreamizInstrumentation.TraceStateHeader)
                        continue;
                    newHeaders.Add(header.Key, header.GetValueBytes());
                }
            }

            newHeaders.Add(StreamizInstrumentation.TraceParentHeader, Encoding.UTF8.GetBytes(activity.Id));
            if (!string.IsNullOrEmpty(activity.TraceStateString))
                newHeaders.Add(StreamizInstrumentation.TraceStateHeader,
                    Encoding.UTF8.GetBytes(activity.TraceStateString));

            return newHeaders;
        }
    }
}
