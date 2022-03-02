using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class TaskMetrics
    {
        private static string AVG_LATENCY_DESCRIPTION = "The average latency of ";
        private static string MAX_LATENCY_DESCRIPTION = "The maximum latency of ";
        private static string RATE_DESCRIPTION_PREFIX = "The average number of ";
        private static string RATE_DESCRIPTION_SUFFIX = " per second";
        private static string ACTIVE_TASK_PREFIX = "active-";

        private static string COMMIT = "commit";
        private static string COMMIT_DESCRIPTION = "calls to commit";
        private static string COMMIT_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + COMMIT_DESCRIPTION;

        private static string COMMIT_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + COMMIT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string PUNCTUATE = "punctuate";
        private static string PUNCTUATE_DESCRIPTION = "calls to punctuate";

        private static string PUNCTUATE_TOTAL_DESCRIPTION =
            StreamMetricsRegistry.TOTAL_DESCRIPTION + PUNCTUATE_DESCRIPTION;

        private static string PUNCTUATE_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + PUNCTUATE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string PUNCTUATE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION + PUNCTUATE_DESCRIPTION;
        private static string PUNCTUATE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION + PUNCTUATE_DESCRIPTION;

        private static string ENFORCED_PROCESSING = "enforced-processing";

        private static string ENFORCED_PROCESSING_TOTAL_DESCRIPTION =
            "The total number of occurrences of enforced-processing operations";

        private static string ENFORCED_PROCESSING_RATE_DESCRIPTION =
            "The average number of occurrences of enforced-processing operations per second";

        private static string RECORD_LATENESS = "record-lateness";

        private static string RECORD_LATENESS_MAX_DESCRIPTION =
            "The observed maximum lateness of records in milliseconds, measured by comparing the record timestamp with the "
            + "current stream time";

        private static string RECORD_LATENESS_AVG_DESCRIPTION =
            "The observed average lateness of records in milliseconds, measured by comparing the record timestamp with the "
            + "current stream time";

        private static string DROPPED_RECORDS = "dropped-records";
        private static string DROPPED_RECORDS_DESCRIPTION = "dropped records";

        private static string DROPPED_RECORDS_TOTAL_DESCRIPTION =
            StreamMetricsRegistry.TOTAL_DESCRIPTION + DROPPED_RECORDS_DESCRIPTION;

        private static string DROPPED_RECORDS_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + DROPPED_RECORDS_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string PROCESS = "process";
        private static string PROCESS_LATENCY = PROCESS + StreamMetricsRegistry.LATENCY_SUFFIX;
        private static string PROCESS_DESCRIPTION = "calls to process";
        private static string PROCESS_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION + PROCESS_DESCRIPTION;
        private static string PROCESS_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION + PROCESS_DESCRIPTION;

        private static string PROCESS_RATIO_DESCRIPTION = "The fraction of time the thread spent " +
                                                          "on processing this task among all assigned active tasks";

        private static string BUFFER_COUNT = "buffer-count";

        private static string NUM_BUFFERED_RECORDS_DESCRIPTION = "The count of buffered records that are polled " +
                                                                 "from consumer and not yet processed for this active task";


        private static string RESTORATION_RECORDS = "restoration-records";
        private static string RESTORATION_RECORDS_DESCRIPTION = "The count of records not restored yet for this active task";
        
        private static string ACTIVE_RESTORATION = "active-restoration";
        private static string ACTIVE_RESTORATION_DESCRIPTION = "Indicate if the active task is in restoration or not";

        #region Process

        public static Sensor ProcessLatencySensor(string threadId, TaskId taskId, StreamMetricsRegistry metricsRegistry)
        {
            return AvgAndMaxSensor(
                threadId,
                taskId,
                PROCESS_LATENCY,
                PROCESS_DESCRIPTION,
                PROCESS_AVG_LATENCY_DESCRIPTION,
                PROCESS_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                metricsRegistry
            );
        }

        public static Sensor EnforcedProcessingSensor(string threadId, TaskId taskId,
            StreamMetricsRegistry streamsMetrics)
        {
            return InvocationRateAndCountSensor(
                threadId,
                taskId,
                ENFORCED_PROCESSING,
                ENFORCED_PROCESSING,
                ENFORCED_PROCESSING_RATE_DESCRIPTION,
                ENFORCED_PROCESSING_TOTAL_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        #endregion

        #region Commit

        public static Sensor CommitSensor(string threadId, TaskId taskId, StreamMetricsRegistry streamsMetrics)
        {
            return InvocationRateAndCountSensor(
                threadId,
                taskId,
                COMMIT,
                COMMIT_DESCRIPTION,
                COMMIT_RATE_DESCRIPTION,
                COMMIT_TOTAL_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        #endregion

        #region Records

        public static Sensor DroppedRecordsSensor(string threadId,
            TaskId taskId,
            StreamMetricsRegistry streamsMetrics)
        {
            return InvocationRateAndCountSensor(
                threadId,
                taskId,
                DROPPED_RECORDS,
                DROPPED_RECORDS_DESCRIPTION,
                DROPPED_RECORDS_RATE_DESCRIPTION,
                DROPPED_RECORDS_TOTAL_DESCRIPTION,
                MetricsRecordingLevel.INFO,
                streamsMetrics
            );
        }

        public static Sensor ActiveBufferedRecordsSensor(
            string threadId,
            TaskId taskId,
            StreamMetricsRegistry metricsRegistry)
        {
            string name = ACTIVE_TASK_PREFIX + BUFFER_COUNT;
            Sensor sensor = metricsRegistry.TaskLevelSensor(threadId, taskId, name, name, MetricsRecordingLevel.DEBUG);
            
            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.TASK_LEVEL_GROUP,
                metricsRegistry.TaskLevelTags(threadId, taskId.ToString()),
                name,
                NUM_BUFFERED_RECORDS_DESCRIPTION
            );
            
            return sensor;
        }

        #endregion

        #region Restoration

        public static Sensor RestorationRecordsSensor(string threadId,
            TaskId taskId,
            StreamMetricsRegistry metricsRegistry)
        {
            Sensor sensor = metricsRegistry.TaskLevelSensor(threadId, taskId, RESTORATION_RECORDS, RESTORATION_RECORDS_DESCRIPTION, MetricsRecordingLevel.DEBUG);
            
            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.TASK_LEVEL_GROUP,
                metricsRegistry.TaskLevelTags(threadId, taskId.ToString()),
                RESTORATION_RECORDS,
                RESTORATION_RECORDS_DESCRIPTION
            );
            
            return sensor;
        }
        
        public static Sensor ActiveRestorationSensor(string threadId,
            TaskId taskId,
            StreamMetricsRegistry metricsRegistry)
        {
            Sensor sensor = metricsRegistry.TaskLevelSensor(threadId, taskId, ACTIVE_RESTORATION, ACTIVE_RESTORATION_DESCRIPTION, MetricsRecordingLevel.DEBUG);
            
            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.TASK_LEVEL_GROUP,
                metricsRegistry.TaskLevelTags(threadId, taskId.ToString()),
                ACTIVE_RESTORATION,
                ACTIVE_RESTORATION_DESCRIPTION
            );
            
            return sensor;
        }

        #endregion

        #region Tools

        private static Sensor InvocationRateAndCountSensor(string threadId,
            TaskId taskId,
            string metricName,
            string metricDescription,
            string descriptionOfRate,
            string descriptionOfCount,
            MetricsRecordingLevel recordingLevel,
            StreamMetricsRegistry streamsMetrics)
        {
            Sensor sensor =
                streamsMetrics.TaskLevelSensor(threadId, taskId, metricName, metricDescription, recordingLevel);

            SensorHelper.AddInvocationRateAndCountToSensor(
                sensor,
                StreamMetricsRegistry.TASK_LEVEL_GROUP,
                streamsMetrics.TaskLevelTags(threadId, taskId.ToString()),
                metricName,
                descriptionOfRate,
                descriptionOfCount
            );

            return sensor;
        }

        private static Sensor AvgAndMaxSensor(string threadId,
            TaskId taskId,
            string metricName,
            string metricDescription,
            string descriptionOfAvg,
            string descriptionOfMax,
            MetricsRecordingLevel recordingLevel,
            StreamMetricsRegistry streamsMetrics)
        {
            Sensor sensor =
                streamsMetrics.TaskLevelSensor(threadId, taskId, metricName, metricDescription, recordingLevel);
            var tags = streamsMetrics.TaskLevelTags(threadId, taskId.ToString());

            SensorHelper.AddAvgAndMaxToSensor(
                sensor,
                StreamMetricsRegistry.TASK_LEVEL_GROUP,
                tags,
                metricName,
                descriptionOfAvg,
                descriptionOfMax
            );

            return sensor;
        }

        #endregion
    }
}