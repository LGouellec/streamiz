using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class ProcessorNodeMetrics
    {
        internal static string RATE_DESCRIPTION_PREFIX = "The average number of ";
        internal static string RATE_DESCRIPTION_SUFFIX = " per second";

        internal static string SUPPRESSION_EMIT = "suppression-emit";
        internal static string SUPPRESSION_EMIT_DESCRIPTION = "emitted records from the suppression buffer";

        internal static readonly string SUPPRESSION_EMIT_TOTAL_DESCRIPTION =
            StreamMetricsRegistry.TOTAL_DESCRIPTION + SUPPRESSION_EMIT_DESCRIPTION;

        internal static readonly string SUPPRESSION_EMIT_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + SUPPRESSION_EMIT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
        
        internal static string PROCESS = "process";
        internal static string PROCESS_DESCRIPTION = "calls to process";
        internal static readonly string PROCESS_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + PROCESS_DESCRIPTION;

        internal static readonly string PROCESS_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + PROCESS_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
            
        internal static string RETRY = "retry";
        internal static string RETRY_DESCRIPTION = "retries";
        internal static readonly string RETRY_AVG_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION_PREFIX + RETRY_DESCRIPTION;
        internal static readonly string RETRY_MAX_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;
        
        public static Sensor SuppressionEmitSensor(string threadId,
            TaskId taskId,
            string processorNodeId,
            StreamMetricsRegistry metricsRegistry)
        {
            return ThroughputSensor(
                threadId,
                taskId,
                processorNodeId,
                SUPPRESSION_EMIT,
                SUPPRESSION_EMIT_DESCRIPTION,
                SUPPRESSION_EMIT_RATE_DESCRIPTION,
                SUPPRESSION_EMIT_TOTAL_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                metricsRegistry
            );
        }

        public static Sensor ProcessNodeSensor(string threadId,
            TaskId taskId,
            string processorNodeId,
            StreamMetricsRegistry metricsRegistry)
        {
            return ThroughputSensor(
                threadId,
                taskId,
                processorNodeId,
                PROCESS,
                PROCESS_DESCRIPTION,
                PROCESS_RATE_DESCRIPTION,
                PROCESS_TOTAL_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                metricsRegistry
            );
        }

        private static Sensor ThroughputSensor(string threadId,
            TaskId taskId,
            string processorNodeId,
            string metricNamePrefix,
            string metricDescription,
            string descriptionOfRate,
            string descriptionOfCount,
            MetricsRecordingLevel recordingLevel,
            StreamMetricsRegistry metricsRegistry)
        {
            Sensor sensor =
                metricsRegistry.NodeLevelSensor(threadId, taskId, processorNodeId, metricNamePrefix, metricDescription,
                    recordingLevel);
            var tags = metricsRegistry.NodeLevelTags(threadId, taskId.ToString(), processorNodeId);

            SensorHelper.AddInvocationRateAndCountToSensor(
                sensor,
                StreamMetricsRegistry.PROCESSOR_NODE_LEVEL_GROUP,
                tags,
                metricNamePrefix,
                descriptionOfRate,
                descriptionOfCount
            );
            return sensor;
        }

        public static Sensor RetrySensor(
            string threadId,
            TaskId taskId,
            string processorNodeId,
            StreamMetricsRegistry metricsRegistry)
        {
            Sensor sensor = metricsRegistry.NodeLevelSensor(threadId, taskId, processorNodeId, RETRY, RETRY_DESCRIPTION, MetricsRecordingLevel.DEBUG);
            var tags = metricsRegistry.NodeLevelTags(threadId, taskId.ToString(), processorNodeId);

            SensorHelper.AddAvgAndMaxToSensor(sensor,
                StreamMetricsRegistry.PROCESSOR_NODE_LEVEL_GROUP,
                tags,
                RETRY,
                RETRY_AVG_DESCRIPTION,
                RETRY_MAX_DESCRIPTION);
            
            return sensor;
        }
    }
}