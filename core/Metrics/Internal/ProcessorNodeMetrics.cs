using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class ProcessorNodeMetrics
    {
        private static string RATE_DESCRIPTION_PREFIX = "The average number of ";
        private static string RATE_DESCRIPTION_SUFFIX = " per second";

        private static string SUPPRESSION_EMIT = "suppression-emit";
        private static string SUPPRESSION_EMIT_DESCRIPTION = "emitted records from the suppression buffer";

        private static string SUPPRESSION_EMIT_TOTAL_DESCRIPTION =
            StreamMetricsRegistry.TOTAL_DESCRIPTION + SUPPRESSION_EMIT_DESCRIPTION;

        private static string SUPPRESSION_EMIT_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + SUPPRESSION_EMIT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string IDEMPOTENT_UPDATE_SKIP = "idempotent-update-skip";
        private static string IDEMPOTENT_UPDATE_SKIP_DESCRIPTION = "skipped idempotent updates";

        private static string IDEMPOTENT_UPDATE_SKIP_TOTAL_DESCRIPTION =
            StreamMetricsRegistry.TOTAL_DESCRIPTION + IDEMPOTENT_UPDATE_SKIP_DESCRIPTION;

        private static string IDEMPOTENT_UPDATE_SKIP_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + IDEMPOTENT_UPDATE_SKIP_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string PROCESS = "process";
        private static string PROCESS_DESCRIPTION = "calls to process";
        private static string PROCESS_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + PROCESS_DESCRIPTION;

        private static string PROCESS_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + PROCESS_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string FORWARD = "forward";
        private static string FORWARD_DESCRIPTION = "calls to forward";
        private static string FORWARD_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + FORWARD_DESCRIPTION;

        private static string FORWARD_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + FORWARD_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

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
    }
}