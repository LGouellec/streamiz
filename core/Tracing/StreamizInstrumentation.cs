namespace Streamiz.Kafka.Net.Tracing
{
    /// <summary>
    /// Public constants of the Streamiz distributed tracing instrumentation.
    /// <para>
    /// Streamiz emits <see cref="System.Diagnostics.Activity"/> (spans) through a <see cref="System.Diagnostics.ActivitySource"/>
    /// named <see cref="ActivitySourceName"/>. Nothing is created unless a listener subscribes to this source
    /// (for instance an OpenTelemetry <c>TracerProvider</c> configured with <c>AddSource("Streamiz.Kafka.Net")</c>
    /// or <c>AddStreamizInstrumentation()</c> from the Streamiz.Kafka.Net.Metrics.OpenTelemetry package).
    /// </para>
    /// <para>
    /// When enabled, Streamiz creates :
    /// <list type="bullet">
    /// <item>one <c>process {topic}</c> span (kind <see cref="System.Diagnostics.ActivityKind.Consumer"/>) per consumed record, covering the whole sub-topology (deserialization up to the sinks).
    /// The W3C trace context found in the record headers (<c>traceparent</c> / <c>tracestate</c>) is used as parent.</item>
    /// <item>one <c>send {topic}</c> span (kind <see cref="System.Diagnostics.ActivityKind.Producer"/>) per record produced by a sink processor (including repartition topics).
    /// Its W3C trace context is injected in the headers of the produced record, so that the next hop continues the trace.</item>
    /// </list>
    /// </para>
    /// </summary>
    public static class StreamizInstrumentation
    {
        /// <summary>
        /// Name of the <see cref="System.Diagnostics.ActivitySource"/> used by Streamiz.
        /// </summary>
        public const string ActivitySourceName = "Streamiz.Kafka.Net";

        /// <summary>
        /// Header key carrying the W3C trace parent.
        /// </summary>
        public const string TraceParentHeader = "traceparent";

        /// <summary>
        /// Header key carrying the W3C trace state.
        /// </summary>
        public const string TraceStateHeader = "tracestate";
    }
}
