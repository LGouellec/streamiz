using System;
using OpenTelemetry.Trace;
using Streamiz.Kafka.Net.Tracing;

namespace Streamiz.Kafka.Net.Metrics.OpenTelemetry
{
    /// <summary>
    /// Extension methods to enable the Streamiz distributed tracing instrumentation in OpenTelemetry.
    /// </summary>
    public static class TracerProviderBuilderExtensions
    {
        /// <summary>
        /// Subscribe the <see cref="TracerProviderBuilder"/> to the Streamiz activity source (<see cref="StreamizInstrumentation.ActivitySourceName"/>).
        /// Once enabled, Streamiz creates one <c>process {topic}</c> span per consumed record (child of the W3C <c>traceparent</c> found in the record headers, if any)
        /// and one <c>send {topic}</c> span per produced record, whose trace context is injected in the produced record headers.
        /// <example>
        /// <code>
        /// using var tracerProvider = Sdk.CreateTracerProviderBuilder()
        ///     .AddStreamizInstrumentation()
        ///     .AddOtlpExporter()
        ///     .Build();
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="builder">OpenTelemetry tracer provider builder</param>
        /// <returns>The same builder, for chaining</returns>
        public static TracerProviderBuilder AddStreamizInstrumentation(this TracerProviderBuilder builder)
        {
            if (builder == null)
                throw new ArgumentNullException(nameof(builder));

            return builder.AddSource(StreamizInstrumentation.ActivitySourceName);
        }
    }
}
