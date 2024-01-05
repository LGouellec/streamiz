namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class ThreadMetrics
    {
        internal static readonly string COMMIT = "commit";
        internal static readonly string POLL = "poll";
        internal static readonly string PROCESS = "process";
        internal static readonly string PUNCTUATE = "punctuate";
        internal static readonly string CREATE_TASK = "task-created";
        internal static readonly string CLOSE_TASK = "task-closed";
        internal static readonly string THREAD_START_TIME = "thread-start-time";

        internal static readonly string COMMIT_DESCRIPTION = "calls to commit";
        internal static readonly string COMMIT_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + COMMIT_DESCRIPTION;
        internal static readonly string COMMIT_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + COMMIT_DESCRIPTION;
        internal static readonly string COMMIT_AVG_LATENCY_DESCRIPTION = "The average commit latency";
        internal static readonly string COMMIT_MAX_LATENCY_DESCRIPTION = "The maximum commit latency";
        internal static readonly string CREATE_TASK_DESCRIPTION = "newly created tasks";
        internal static readonly string CREATE_TASK_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + CREATE_TASK_DESCRIPTION;
        internal static readonly string CREATE_TASK_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + CREATE_TASK_DESCRIPTION;
        internal static readonly string CLOSE_TASK_DESCRIPTION = "closed tasks";
        internal static readonly string CLOSE_TASK_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + CLOSE_TASK_DESCRIPTION;
        internal static readonly string CLOSE_TASK_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + CLOSE_TASK_DESCRIPTION;
        internal static readonly string POLL_DESCRIPTION = "calls to poll";
        internal static readonly string POLL_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + POLL_DESCRIPTION;
        internal static readonly string POLL_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + POLL_DESCRIPTION;
        internal static readonly string POLL_AVG_LATENCY_DESCRIPTION = "The average poll latency";
        internal static readonly string POLL_MAX_LATENCY_DESCRIPTION = "The maximum poll latency";
        internal static readonly string POLL_AVG_RECORDS_DESCRIPTION = "The average number of records polled from consumer within an iteration";
        internal static readonly string POLL_MAX_RECORDS_DESCRIPTION = "The maximum number of records polled from consumer within an iteration";
        internal static readonly string PROCESS_DESCRIPTION = "calls to process";
        internal static readonly string PROCESS_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + PROCESS_DESCRIPTION;
        internal static readonly string PROCESS_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + PROCESS_DESCRIPTION;
        internal static readonly string PROCESS_AVG_LATENCY_DESCRIPTION = "The average process latency";
        internal static readonly string PROCESS_MAX_LATENCY_DESCRIPTION = "The maximum process latency";
        internal static readonly string PROCESS_AVG_RECORDS_DESCRIPTION = "The average number of records processed within an iteration";
        internal static readonly string PROCESS_MAX_RECORDS_DESCRIPTION = "The maximum number of records processed within an iteration";
        internal static readonly string PUNCTUATE_DESCRIPTION = "calls to punctuate";
        internal static readonly string PUNCTUATE_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + PUNCTUATE_DESCRIPTION;
        internal static readonly string PUNCTUATE_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + PUNCTUATE_DESCRIPTION;
        internal static readonly string PUNCTUATE_AVG_LATENCY_DESCRIPTION = "The average punctuate latency";
        internal static readonly string PUNCTUATE_MAX_LATENCY_DESCRIPTION = "The maximum punctuate latency";
        internal static readonly string COMMIT_OVER_TASKS_DESCRIPTION =
            "calls to commit over all tasks assigned to one stream thread";
        internal static readonly string COMMIT_OVER_TASKS_TOTAL_DESCRIPTION = StreamMetricsRegistry.TOTAL_DESCRIPTION + COMMIT_OVER_TASKS_DESCRIPTION;
        internal static readonly string COMMIT_OVER_TASKS_RATE_DESCRIPTION = StreamMetricsRegistry.RATE_DESCRIPTION + COMMIT_OVER_TASKS_DESCRIPTION;
        internal static readonly string PROCESS_RATIO_DESCRIPTION =
            "The fraction of time the thread spent on processing active tasks";
        internal static readonly string POLL_RATIO_DESCRIPTION =
            "The fraction of time the thread spent on polling records from consumer";
        internal static readonly string COMMIT_RATIO_DESCRIPTION =
            "The fraction of time the thread spent on committing all tasks";
        internal static readonly string PUNCTUATE_RATIO_DESCRIPTION =
            "The fraction of time the thread spent on punctuating active tasks";
        internal static readonly string THREAD_START_TIME_DESCRIPTION =
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
            return SensorHelper.InvocationRateAndCountSensor(
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
            return SensorHelper.InvocationRateAndCountSensor(
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
            return SensorHelper.InvocationRateAndCountAndAvgAndMaxLatencySensor(
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
            return SensorHelper.InvocationRateAndCountAndAvgAndMaxLatencySensor(
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
        
        #region Punctuate
        
        public static Sensor PunctuateSensor(string threadId, StreamMetricsRegistry metricsRegistry) 
        {
            return SensorHelper.InvocationRateAndCountAndAvgAndMaxLatencySensor(
                threadId,
                PUNCTUATE,
                PUNCTUATE_DESCRIPTION,
                PUNCTUATE_RATE_DESCRIPTION,
                PUNCTUATE_TOTAL_DESCRIPTION,
                PUNCTUATE_AVG_LATENCY_DESCRIPTION,
                PUNCTUATE_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.INFO,
                metricsRegistry
            );
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
        
        public static Sensor PunctuateRatioSensor(string threadId, StreamMetricsRegistry metricsRegistry) {
            
            var sensor =
                metricsRegistry.ThreadLevelSensor(
                    threadId, 
                    PUNCTUATE + StreamMetricsRegistry.RATIO_SUFFIX,
                    PUNCTUATE_RATIO_DESCRIPTION,
                    MetricsRecordingLevel.INFO);
            var tags = metricsRegistry.ThreadLevelTags(threadId);
            
            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                tags,
                PUNCTUATE + StreamMetricsRegistry.RATIO_SUFFIX,
                PUNCTUATE_RATIO_DESCRIPTION);
            
            return sensor;
        }
        
        #endregion
    }
}