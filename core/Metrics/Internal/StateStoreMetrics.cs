using System.Collections.Generic;
using System.Threading;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class StateStoreMetrics
    {
        internal static string AVG_DESCRIPTION_PREFIX = "The average ";
        internal static string MAX_DESCRIPTION_PREFIX = "The maximum ";
        internal static string LATENCY_DESCRIPTION = "latency of ";
        internal static readonly string AVG_LATENCY_DESCRIPTION_PREFIX = AVG_DESCRIPTION_PREFIX + LATENCY_DESCRIPTION;
        internal static readonly string MAX_LATENCY_DESCRIPTION_PREFIX = MAX_DESCRIPTION_PREFIX + LATENCY_DESCRIPTION;
        internal static string RATE_DESCRIPTION_PREFIX = "The average number of ";
        internal static string RATE_DESCRIPTION_SUFFIX = " per second";
        internal static string BUFFERED_RECORDS = "buffered records";

        internal static string PUT = "put";
        internal static string PUT_DESCRIPTION = "calls to put";

        internal static readonly string PUT_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + PUT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        internal static readonly string PUT_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + PUT_DESCRIPTION;
        internal static readonly string PUT_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + PUT_DESCRIPTION;

        internal static string PUT_IF_ABSENT = "put-if-absent";
        internal static string PUT_IF_ABSENT_DESCRIPTION = "calls to put-if-absent";

        internal static readonly string PUT_IF_ABSENT_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + PUT_IF_ABSENT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        internal static readonly string PUT_IF_ABSENT_AVG_LATENCY_DESCRIPTION =
            AVG_LATENCY_DESCRIPTION_PREFIX + PUT_IF_ABSENT_DESCRIPTION;

        internal static readonly string PUT_IF_ABSENT_MAX_LATENCY_DESCRIPTION =
            MAX_LATENCY_DESCRIPTION_PREFIX + PUT_IF_ABSENT_DESCRIPTION;

        internal static string PUT_ALL = "put-all";
        internal static string PUT_ALL_DESCRIPTION = "calls to put-all";

        internal static readonly string PUT_ALL_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + PUT_ALL_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        internal static readonly string PUT_ALL_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + PUT_ALL_DESCRIPTION;
        internal static readonly string PUT_ALL_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + PUT_ALL_DESCRIPTION;

        internal static string GET = "get";
        internal static string GET_DESCRIPTION = "calls to get";

        internal static readonly string GET_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + GET_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        internal static readonly string GET_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + GET_DESCRIPTION;
        internal static readonly string GET_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + GET_DESCRIPTION;

        internal static string FETCH = "fetch";
        internal static string FETCH_DESCRIPTION = "calls to fetch";

        internal static readonly string FETCH_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + FETCH_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        internal static readonly string FETCH_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + FETCH_DESCRIPTION;
        internal static readonly string FETCH_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + FETCH_DESCRIPTION;

        internal static string ALL = "all";
        internal static string ALL_DESCRIPTION = "calls to all";

        internal static string ALL_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + ALL_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        internal static readonly string ALL_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + ALL_DESCRIPTION;
        internal static readonly string ALL_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + ALL_DESCRIPTION;

        internal static string RANGE = "range";
        internal static string RANGE_DESCRIPTION = "calls to range";

        internal static readonly string RANGE_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + RANGE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        internal static readonly string RANGE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + RANGE_DESCRIPTION;
        internal static readonly string RANGE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + RANGE_DESCRIPTION;

        internal static string FLUSH = "flush";
        internal static string FLUSH_DESCRIPTION = "calls to flush";
        internal static readonly string FLUSH_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + FLUSH_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        internal static readonly string FLUSH_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + FLUSH_DESCRIPTION;
        internal static readonly string FLUSH_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + FLUSH_DESCRIPTION;

        internal static string DELETE = "delete";
        internal static string DELETE_DESCRIPTION = "calls to delete";

        internal static readonly string DELETE_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + DELETE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        internal static readonly string DELETE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + DELETE_DESCRIPTION;
        internal static readonly string DELETE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + DELETE_DESCRIPTION;

        internal static string REMOVE = "remove";
        internal static string REMOVE_DESCRIPTION = "calls to remove";

        internal static readonly string REMOVE_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + REMOVE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        internal static readonly string REMOVE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + REMOVE_DESCRIPTION;
        internal static readonly string REMOVE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + REMOVE_DESCRIPTION;

        internal static string RESTORE = "restore";
        internal static string RESTORE_DESCRIPTION = "restorations";

        internal static readonly string RESTORE_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + RESTORE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        internal static readonly string RESTORE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + RESTORE_DESCRIPTION;
        internal static readonly string RESTORE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + RESTORE_DESCRIPTION;

        internal static string SUPPRESSION_BUFFER_COUNT = "suppression-buffer-count";
        internal static readonly string SUPPRESSION_BUFFER_COUNT_DESCRIPTION = "count of " + BUFFERED_RECORDS;

        internal static readonly string SUPPRESSION_BUFFER_COUNT_AVG_DESCRIPTION =
            AVG_DESCRIPTION_PREFIX + SUPPRESSION_BUFFER_COUNT_DESCRIPTION;

        internal static readonly string SUPPRESSION_BUFFER_COUNT_MAX_DESCRIPTION =
            MAX_DESCRIPTION_PREFIX + SUPPRESSION_BUFFER_COUNT_DESCRIPTION;

        internal static string SUPPRESSION_BUFFER_SIZE = "suppression-buffer-size";
        internal static readonly string SUPPRESSION_BUFFER_SIZE_DESCRIPTION = "size of " + BUFFERED_RECORDS;

        internal static readonly string SUPPRESSION_BUFFER_SIZE_AVG_DESCRIPTION =
            AVG_DESCRIPTION_PREFIX + SUPPRESSION_BUFFER_SIZE_DESCRIPTION;

        internal static readonly string SUPPRESSION_BUFFER_SIZE_MAX_DESCRIPTION =
            MAX_DESCRIPTION_PREFIX + SUPPRESSION_BUFFER_SIZE_DESCRIPTION;

        internal static string EXPIRED_WINDOW_RECORD_DROP = "expired-window-record-drop";
        internal static string EXPIRED_WINDOW_RECORD_DROP_DESCRIPTION = "dropped records due to an expired window";

        internal static readonly string EXPIRED_WINDOW_RECORD_DROP_TOTAL_DESCRIPTION =
            StreamMetricsRegistry.TOTAL_DESCRIPTION + EXPIRED_WINDOW_RECORD_DROP_DESCRIPTION;

        internal static readonly string EXPIRED_WINDOW_RECORD_DROP_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + EXPIRED_WINDOW_RECORD_DROP_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        public static Sensor PutSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return ThroughputAndLatencySensor(
                taskId,
                storeType,
                storeName,
                PUT,
                PUT_DESCRIPTION,
                PUT_RATE_DESCRIPTION,
                PUT_AVG_LATENCY_DESCRIPTION,
                PUT_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        public static Sensor PutIfAbsentSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return ThroughputAndLatencySensor(
                taskId,
                storeType,
                storeName,
                PUT_IF_ABSENT,
                PUT_IF_ABSENT_DESCRIPTION,
                PUT_IF_ABSENT_RATE_DESCRIPTION,
                PUT_IF_ABSENT_AVG_LATENCY_DESCRIPTION,
                PUT_IF_ABSENT_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        public static Sensor PutAllSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return ThroughputAndLatencySensor(
                taskId,
                storeType,
                storeName,
                PUT_ALL,
                PUT_ALL_DESCRIPTION,
                PUT_ALL_RATE_DESCRIPTION,
                PUT_ALL_AVG_LATENCY_DESCRIPTION,
                PUT_ALL_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        public static Sensor GetSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return ThroughputAndLatencySensor(
                taskId,
                storeType,
                storeName,
                GET,
                GET_DESCRIPTION,
                GET_RATE_DESCRIPTION,
                GET_AVG_LATENCY_DESCRIPTION,
                GET_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        public static Sensor FetchSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return ThroughputAndLatencySensor(
                taskId,
                storeType,
                storeName,
                FETCH,
                FETCH_DESCRIPTION,
                FETCH_RATE_DESCRIPTION,
                FETCH_AVG_LATENCY_DESCRIPTION,
                FETCH_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        public static Sensor AllSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return ThroughputAndLatencySensor(
                taskId,
                storeType,
                storeName,
                ALL,
                ALL_DESCRIPTION,
                ALL_RATE_DESCRIPTION,
                ALL_AVG_LATENCY_DESCRIPTION,
                ALL_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        public static Sensor RangeSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return ThroughputAndLatencySensor(
                taskId,
                storeType,
                storeName,
                RANGE,
                RANGE_DESCRIPTION,
                RANGE_RATE_DESCRIPTION,
                RANGE_AVG_LATENCY_DESCRIPTION,
                RANGE_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }
        
        public static Sensor FlushSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return ThroughputAndLatencySensor(
                taskId,
                storeType,
                storeName,
                FLUSH,
                FLUSH_DESCRIPTION,
                FLUSH_RATE_DESCRIPTION,
                FLUSH_AVG_LATENCY_DESCRIPTION,
                FLUSH_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        public static Sensor DeleteSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return ThroughputAndLatencySensor(
                taskId,
                storeType,
                storeName,
                DELETE,
                DELETE_DESCRIPTION,
                DELETE_RATE_DESCRIPTION,
                DELETE_AVG_LATENCY_DESCRIPTION,
                DELETE_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        public static Sensor RemoveSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return ThroughputAndLatencySensor(
                taskId,
                storeType,
                storeName,
                REMOVE,
                REMOVE_DESCRIPTION,
                REMOVE_RATE_DESCRIPTION,
                REMOVE_AVG_LATENCY_DESCRIPTION,
                REMOVE_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        public static Sensor RestoreSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return ThroughputAndLatencySensor(
                taskId, storeType,
                storeName,
                RESTORE,
                RESTORE_DESCRIPTION,
                RESTORE_RATE_DESCRIPTION,
                RESTORE_AVG_LATENCY_DESCRIPTION,
                RESTORE_MAX_LATENCY_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        public static Sensor ExpiredWindowRecordDropSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            Sensor sensor = streamsMetrics.StoreLevelSensor(
                GetThreadId(),
                taskId,
                storeName,
                EXPIRED_WINDOW_RECORD_DROP,
                EXPIRED_WINDOW_RECORD_DROP_DESCRIPTION,
                MetricsRecordingLevel.INFO
            );
            
            SensorHelper.AddInvocationRateAndCountToSensor(
                sensor,
                "stream-" + storeType + "-metrics",
                streamsMetrics.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType),
                EXPIRED_WINDOW_RECORD_DROP,
                EXPIRED_WINDOW_RECORD_DROP_RATE_DESCRIPTION,
                EXPIRED_WINDOW_RECORD_DROP_TOTAL_DESCRIPTION
            );
            return sensor;
        }

        public static Sensor SuppressionBufferCountSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return SizeOrCountSensor(
                taskId,
                storeType,
                storeName,
                SUPPRESSION_BUFFER_COUNT,
                SUPPRESSION_BUFFER_COUNT_DESCRIPTION,
                SUPPRESSION_BUFFER_COUNT_AVG_DESCRIPTION,
                SUPPRESSION_BUFFER_COUNT_MAX_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }

        public static Sensor SuppressionBufferSizeSensor(TaskId taskId,
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics)
        {
            return SizeOrCountSensor(
                taskId,
                storeType,
                storeName,
                SUPPRESSION_BUFFER_SIZE,
                SUPPRESSION_BUFFER_SIZE_DESCRIPTION,
                SUPPRESSION_BUFFER_SIZE_AVG_DESCRIPTION,
                SUPPRESSION_BUFFER_SIZE_MAX_DESCRIPTION,
                MetricsRecordingLevel.DEBUG,
                streamsMetrics
            );
        }
        
        private static Sensor SizeOrCountSensor(TaskId taskId,
            string storeType,
            string storeName,
            string metricName,
            string metricDescription,
            string descriptionOfAvg,
            string descriptionOfMax,
            MetricsRecordingLevel recordingLevel,
            StreamMetricsRegistry streamsMetrics)
        {
            Sensor sensor = streamsMetrics.StoreLevelSensor(
                GetThreadId(),
                taskId,
                storeName, 
                metricName,
                metricDescription,
                recordingLevel);
            string group;
            IDictionary<string, string> tags;
            group = StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP;
            tags = streamsMetrics.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddAvgAndMaxToSensor(sensor, group, tags, metricName, descriptionOfAvg, descriptionOfMax);
            
            return sensor;
        }

        private static Sensor ThroughputAndLatencySensor(TaskId taskId,
            string storeType,
            string storeName,
            string metricName,
            string metricDescription,
            string descriptionOfRate,
            string descriptionOfAvg,
            string descriptionOfMax,
            MetricsRecordingLevel recordingLevel,
            StreamMetricsRegistry streamsMetrics)
        {
            Sensor sensor;
            string latencyMetricName = metricName + StreamMetricsRegistry.LATENCY_SUFFIX;
            IDictionary<string, string> tags =
                streamsMetrics.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            sensor = streamsMetrics.StoreLevelSensor(GetThreadId(), taskId, storeName, metricName, metricDescription, recordingLevel);
            SensorHelper.AddInvocationRateToSensor(sensor, StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP, tags, metricName,
                descriptionOfRate);
            
            SensorHelper.AddAvgAndMaxToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                latencyMetricName,
                descriptionOfAvg,
                descriptionOfMax
            );
            return sensor;
        }

        private static string GetThreadId() => Thread.CurrentThread.Name;
    }
}