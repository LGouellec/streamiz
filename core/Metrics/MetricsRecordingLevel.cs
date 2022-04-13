namespace Streamiz.Kafka.Net.Metrics
{
    /// <summary>
    /// MetricsRecordingLevel enum
    /// <para>
    /// - INFO = list high level metrics (client, thread, task)
    /// - DEBUG = list all metrics (client, thread, task, processor, state-store)
    /// </para> 
    /// </summary>
    public enum MetricsRecordingLevel
    {
        /// <summary>
        /// Info level metrics
        /// </summary>
        INFO,
        /// <summary>
        /// Debug level metrics
        /// </summary>
        DEBUG 
    }
}