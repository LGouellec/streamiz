namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class ThreadMetrics
    {
        private static readonly string COMMIT = "commit";
        private static readonly string POLL = "poll";
        private static readonly string PROCESS = "process";
        private static readonly string PUNCTUATE = "punctuate";
        private static readonly string CREATE_TASK = "task-created";
        private static readonly string CLOSE_TASK = "task-closed";
        private static readonly string THREAD_START_TIME = "thread-start-time";

        private static readonly string COMMIT_DESCRIPTION = "calls to commit";
        private static readonly string COMMIT_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + COMMIT_DESCRIPTION;
        private static readonly string COMMIT_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + COMMIT_DESCRIPTION;
        private static readonly string COMMIT_AVG_LATENCY_DESCRIPTION = "The average commit latency";
        private static readonly string COMMIT_MAX_LATENCY_DESCRIPTION = "The maximum commit latency";
        private static readonly string CREATE_TASK_DESCRIPTION = "newly created tasks";
        private static readonly string CREATE_TASK_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + CREATE_TASK_DESCRIPTION;
        private static readonly string CREATE_TASK_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + CREATE_TASK_DESCRIPTION;
        private static readonly string CLOSE_TASK_DESCRIPTION = "closed tasks";
        private static readonly string CLOSE_TASK_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + CLOSE_TASK_DESCRIPTION;
        private static readonly string CLOSE_TASK_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + CLOSE_TASK_DESCRIPTION;
        private static readonly string POLL_DESCRIPTION = "calls to poll";
        private static readonly string POLL_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + POLL_DESCRIPTION;
        private static readonly string POLL_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + POLL_DESCRIPTION;
        private static readonly string POLL_AVG_LATENCY_DESCRIPTION = "The average poll latency";
        private static readonly string POLL_MAX_LATENCY_DESCRIPTION = "The maximum poll latency";
        private static readonly string POLL_AVG_RECORDS_DESCRIPTION = "The average number of records polled from consumer within an iteration";
        private static readonly string POLL_MAX_RECORDS_DESCRIPTION = "The maximum number of records polled from consumer within an iteration";
        private static readonly string PROCESS_DESCRIPTION = "calls to process";
        private static readonly string PROCESS_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + PROCESS_DESCRIPTION;
        private static readonly string PROCESS_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + PROCESS_DESCRIPTION;
        private static readonly string PROCESS_AVG_LATENCY_DESCRIPTION = "The average process latency";
        private static readonly string PROCESS_MAX_LATENCY_DESCRIPTION = "The maximum process latency";
        private static readonly string PROCESS_AVG_RECORDS_DESCRIPTION = "The average number of records processed within an iteration";
        private static readonly string PROCESS_MAX_RECORDS_DESCRIPTION = "The maximum number of records processed within an iteration";
        private static readonly string PUNCTUATE_DESCRIPTION = "calls to punctuate";
        private static readonly string PUNCTUATE_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + PUNCTUATE_DESCRIPTION;
        private static readonly string PUNCTUATE_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + PUNCTUATE_DESCRIPTION;
        private static readonly string PUNCTUATE_AVG_LATENCY_DESCRIPTION = "The average punctuate latency";
        private static readonly string PUNCTUATE_MAX_LATENCY_DESCRIPTION = "The maximum punctuate latency";
        private static readonly string COMMIT_OVER_TASKS_DESCRIPTION =
            "calls to commit over all tasks assigned to one stream thread";
        private static readonly string COMMIT_OVER_TASKS_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + COMMIT_OVER_TASKS_DESCRIPTION;
        private static readonly string COMMIT_OVER_TASKS_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + COMMIT_OVER_TASKS_DESCRIPTION;
        private static readonly string PROCESS_RATIO_DESCRIPTION =
            "The fraction of time the thread spent on processing active tasks";
        private static readonly string POLL_RATIO_DESCRIPTION =
            "The fraction of time the thread spent on polling records from consumer";
        private static readonly string COMMIT_RATIO_DESCRIPTION =
            "The fraction of time the thread spent on committing all tasks";
        private static readonly string THREAD_START_TIME_DESCRIPTION =
            "The time that the thread was started";
        
        #region Thread Metrics

        public static Sensor CreateStartThreadSensor(string threadId, long startTime, StreamMetricsRegistry metricsRegistry)
        {
            Sensor sensor = metricsRegistry.ThreadLevelSensor(
                threadId,
                THREAD_START_TIME,
                THREAD_START_TIME_DESCRIPTION,
                MetricsRecordingLevel.INFO);
            
            var metricName = new MetricName(THREAD_START_TIME, StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                THREAD_START_TIME_DESCRIPTION, metricsRegistry.ThreadLevelTags(threadId));

            sensor.AddImmutableMetric(metricName, startTime);
            
            return sensor;
        }
        
        #endregion
        
        #region Create & Close Task

        public static Sensor CreateTaskSensor(string threadId, StreamMetricsRegistry metricsRegistry)
        {
            return InvocationRateAndCountSensor(
                threadId,
                CREATE_TASK,
                CREATE_TASK_DESCRIPTION,
                CREATE_TASK_RATE_DESCRIPTION,
                CREATE_TASK_TOTAL_DESCRIPTION,
                MetricsRecordingLevel.INFO,
                metricsRegistry);
        }

        public static Sensor ClosedTaskSensor(string threadId, StreamMetricsRegistry metricsRegistry)
        {
            return InvocationRateAndCountSensor(
                threadId,
                CLOSE_TASK,
                CLOSE_TASK_DESCRIPTION,
                CLOSE_TASK_RATE_DESCRIPTION,
                CLOSE_TASK_TOTAL_DESCRIPTION,
                MetricsRecordingLevel.INFO,
                metricsRegistry);
        }
        
        #endregion
        
        #region Commit

        public static Sensor CommitSensor(string threadId, StreamMetricsRegistry metricsRegistry)
        {
            return InvocationRateAndCountAndAvgAndMaxLatencySensor(
                threadId,
                COMMIT,
                COMMIT_DESCRIPTION,
                COMMIT_RATE_DESCRIPTION,
                COMMIT_TOTAL_DESCRIPTION,
                COMMIT_AVG_LATENCY_DESCRIPTION,
                COMMIT_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.INFO,
                metricsRegistry
            );
        }
        
        #endregion
        
        #region Poll

        public static Sensor PollSensor(string threadId, StreamMetricsRegistry metricsRegistry)
        {
            return InvocationRateAndCountAndAvgAndMaxLatencySensor(
                threadId,
                POLL,
                POLL_DESCRIPTION,
                POLL_RATE_DESCRIPTION,
                POLL_TOTAL_DESCRIPTION,
                POLL_AVG_LATENCY_DESCRIPTION,
                POLL_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.INFO,
                metricsRegistry);
        }
        
        public static Sensor PollRecordsSensor(string threadId, StreamMetricsRegistry metricsRegistry)
        {
            var sensor = metricsRegistry.ThreadLevelSensor(
                threadId, 
                POLL + StreamMetricsRegistry.RECORDS_SUFFIX,
                "number of records polled",
                MetricsRecordingLevel.INFO);
            var tags = metricsRegistry.ThreadLevelTags(threadId);
            
            SensorHelper.AddAvgAndMaxToSensor(
                sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                tags,
                POLL + StreamMetricsRegistry.RECORDS_SUFFIX,
                POLL_AVG_RECORDS_DESCRIPTION,
                POLL_MAX_RECORDS_DESCRIPTION);
            
            return sensor;
        }
        
        #endregion
        
        #region Process
        
        public static Sensor ProcessRecordsSensor(string threadId, StreamMetricsRegistry metricsRegistry)
        {
            var sensor = metricsRegistry.ThreadLevelSensor(
                threadId, 
                PROCESS + StreamMetricsRegistry.RECORDS_SUFFIX,
                PROCESS_DESCRIPTION,
                MetricsRecordingLevel.INFO);
            var tags = metricsRegistry.ThreadLevelTags(threadId);
            
            SensorHelper.AddAvgAndMaxToSensor(
                sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                tags,
                PROCESS + StreamMetricsRegistry.RECORDS_SUFFIX,
                PROCESS_AVG_RECORDS_DESCRIPTION,
                PROCESS_MAX_RECORDS_DESCRIPTION);
            
            return sensor;
        }
        
        public static Sensor ProcessRateSensor(string threadId, StreamMetricsRegistry metricsRegistry)
        {
            var sensor = metricsRegistry.ThreadLevelSensor(
                threadId, 
                PROCESS + StreamMetricsRegistry.RATE_SUFFIX,
                PROCESS_RATE_DESCRIPTION,
                MetricsRecordingLevel.INFO);
            var tags = metricsRegistry.ThreadLevelTags(threadId);
            
            SensorHelper.AddRateOfSumAndSumMetricsToSensor(
                sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                tags,
                PROCESS + StreamMetricsRegistry.RATE_SUFFIX,
                PROCESS_RATE_DESCRIPTION,
                PROCESS_TOTAL_DESCRIPTION);
            
            return sensor;
        }
        
        public static Sensor ProcessLatencySensor(string threadId, StreamMetricsRegistry metricsRegistry) {
            
             Sensor sensor =
                 metricsRegistry.ThreadLevelSensor(
                     threadId, 
                     PROCESS + StreamMetricsRegistry.LATENCY_SUFFIX,
                     PROCESS_DESCRIPTION,
                     MetricsRecordingLevel.INFO);
             
            var tags = metricsRegistry.ThreadLevelTags(threadId);
            
            SensorHelper.AddAvgAndMaxToSensor(
                sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                tags,
                PROCESS + StreamMetricsRegistry.LATENCY_SUFFIX,
                PROCESS_AVG_LATENCY_DESCRIPTION,
                PROCESS_MAX_LATENCY_DESCRIPTION
            );
            
            return sensor;
        }
            
        #endregion
        
        #region Ratio

        public static Sensor ProcessRatioSensor(string threadId, StreamMetricsRegistry metricsRegistry)
        {
            var sensor = metricsRegistry.ThreadLevelSensor(threadId,
                PROCESS + StreamMetricsRegistry.RATIO_SUFFIX,
                PROCESS_RATIO_DESCRIPTION,
                MetricsRecordingLevel.INFO);
            var tags = metricsRegistry.ThreadLevelTags(threadId);
            
            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                tags,
                PROCESS + StreamMetricsRegistry.RATIO_SUFFIX,
                PROCESS_RATIO_DESCRIPTION);
            
            return sensor;
        }
        
        public static Sensor PollRatioSensor(string threadId, StreamMetricsRegistry metricsRegistry)
        {
            var sensor = metricsRegistry.ThreadLevelSensor(threadId,
                POLL + StreamMetricsRegistry.RATIO_SUFFIX,
                POLL_RATIO_DESCRIPTION,
                MetricsRecordingLevel.INFO);
            var tags = metricsRegistry.ThreadLevelTags(threadId);
            
            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                tags,
                POLL + StreamMetricsRegistry.RATIO_SUFFIX,
                POLL_RATIO_DESCRIPTION);
            
            return sensor;
        }
        
        public static Sensor CommitRatioSensor(string threadId, StreamMetricsRegistry metricsRegistry)
        {
            var sensor = metricsRegistry.ThreadLevelSensor(threadId,
                COMMIT + StreamMetricsRegistry.RATIO_SUFFIX,
                COMMIT_RATIO_DESCRIPTION,
                MetricsRecordingLevel.INFO);
            var tags = metricsRegistry.ThreadLevelTags(threadId);
            
            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                tags,
                COMMIT + StreamMetricsRegistry.RATIO_SUFFIX,
                COMMIT_RATIO_DESCRIPTION);
            
            return sensor;
        }
        
        #endregion
        
        #region Tools
        
        #region RateAndCount
        
        internal static Sensor InvocationRateAndCountSensor(string threadId,
                         string metricName,
                         string metricDescription,
                         string descriptionOfRate,
                         string descriptionOfCount,
                         MetricsRecordingLevel recordingLevel,
                         StreamMetricsRegistry streamsMetrics) {
            
            Sensor sensor = streamsMetrics.ThreadLevelSensor(threadId, metricName, metricDescription, recordingLevel);
            
            SensorHelper.AddInvocationRateAndCountToSensor(
                sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                streamsMetrics.ThreadLevelTags(threadId),
                metricName,
                descriptionOfRate,
                descriptionOfCount
            );
            return sensor;
        }
        
        #endregion
            
        #region Rate & Count and Avg & Max latency
        
        internal static Sensor InvocationRateAndCountAndAvgAndMaxLatencySensor( string threadId,
                                 string metricName,
                                 string metricDescription,
                                 string descriptionOfRate,
                                 string descriptionOfCount,
                                 string descriptionOfAvg,
                                 string descriptionOfMax,
                                 MetricsRecordingLevel recordingLevel,
                                 StreamMetricsRegistry streamsMetrics) {
            
            Sensor sensor = streamsMetrics.ThreadLevelSensor(threadId, metricName, metricDescription, recordingLevel);
            var tags = streamsMetrics.ThreadLevelTags(threadId);

            SensorHelper.AddAvgAndMaxToSensor(sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                tags,
                metricName + StreamMetricsRegistry.LATENCY_SUFFIX,
                descriptionOfAvg,
                descriptionOfMax);

            SensorHelper.AddInvocationRateAndCountToSensor(sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                tags,
                metricName,
                descriptionOfRate,
                descriptionOfCount);
            
            return sensor;
        }
        
        #endregion
        
        #endregion
    }
}