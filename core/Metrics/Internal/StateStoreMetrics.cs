using System.Collections.Generic;
using System.Threading;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class StateStoreMetrics
    {
        private static string AVG_DESCRIPTION_PREFIX = "The average ";
        private static string MAX_DESCRIPTION_PREFIX = "The maximum ";
        private static string LATENCY_DESCRIPTION = "latency of ";
        private static string AVG_LATENCY_DESCRIPTION_PREFIX = AVG_DESCRIPTION_PREFIX + LATENCY_DESCRIPTION;
        private static string MAX_LATENCY_DESCRIPTION_PREFIX = MAX_DESCRIPTION_PREFIX + LATENCY_DESCRIPTION;
        private static string RATE_DESCRIPTION_PREFIX = "The average number of ";
        private static string RATE_DESCRIPTION_SUFFIX = " per second";
        private static string BUFFERED_RECORDS = "buffered records";

        private static string PUT = "put";
        private static string PUT_DESCRIPTION = "calls to put";

        private static string PUT_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + PUT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string PUT_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + PUT_DESCRIPTION;
        private static string PUT_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + PUT_DESCRIPTION;

        private static string PUT_IF_ABSENT = "put-if-absent";
        private static string PUT_IF_ABSENT_DESCRIPTION = "calls to put-if-absent";

        private static string PUT_IF_ABSENT_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + PUT_IF_ABSENT_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string PUT_IF_ABSENT_AVG_LATENCY_DESCRIPTION =
            AVG_LATENCY_DESCRIPTION_PREFIX + PUT_IF_ABSENT_DESCRIPTION;

        private static string PUT_IF_ABSENT_MAX_LATENCY_DESCRIPTION =
            MAX_LATENCY_DESCRIPTION_PREFIX + PUT_IF_ABSENT_DESCRIPTION;

        private static string PUT_ALL = "put-all";
        private static string PUT_ALL_DESCRIPTION = "calls to put-all";

        private static string PUT_ALL_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + PUT_ALL_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string PUT_ALL_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + PUT_ALL_DESCRIPTION;
        private static string PUT_ALL_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + PUT_ALL_DESCRIPTION;

        private static string GET = "get";
        private static string GET_DESCRIPTION = "calls to get";

        private static string GET_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + GET_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string GET_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + GET_DESCRIPTION;
        private static string GET_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + GET_DESCRIPTION;

        private static string FETCH = "fetch";
        private static string FETCH_DESCRIPTION = "calls to fetch";

        private static string FETCH_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + FETCH_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string FETCH_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + FETCH_DESCRIPTION;
        private static string FETCH_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + FETCH_DESCRIPTION;

        private static string ALL = "all";
        private static string ALL_DESCRIPTION = "calls to all";

        private static string ALL_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + ALL_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string ALL_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + ALL_DESCRIPTION;
        private static string ALL_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + ALL_DESCRIPTION;

        private static string RANGE = "range";
        private static string RANGE_DESCRIPTION = "calls to range";

        private static string RANGE_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + RANGE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string RANGE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + RANGE_DESCRIPTION;
        private static string RANGE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + RANGE_DESCRIPTION;

        private static string PREFIX_SCAN = "prefix-scan";
        private static string PREFIX_SCAN_DESCRIPTION = "calls to prefix-scan";

        private static string PREFIX_SCAN_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + PREFIX_SCAN_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string PREFIX_SCAN_AVG_LATENCY_DESCRIPTION =
            AVG_LATENCY_DESCRIPTION_PREFIX + PREFIX_SCAN_DESCRIPTION;

        private static string PREFIX_SCAN_MAX_LATENCY_DESCRIPTION =
            MAX_LATENCY_DESCRIPTION_PREFIX + PREFIX_SCAN_DESCRIPTION;

        private static string FLUSH = "flush";
        private static string FLUSH_DESCRIPTION = "calls to flush";

        private static string FLUSH_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + FLUSH_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string FLUSH_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + FLUSH_DESCRIPTION;
        private static string FLUSH_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + FLUSH_DESCRIPTION;

        private static string DELETE = "delete";
        private static string DELETE_DESCRIPTION = "calls to delete";

        private static string DELETE_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + DELETE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string DELETE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + DELETE_DESCRIPTION;
        private static string DELETE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + DELETE_DESCRIPTION;

        private static string REMOVE = "remove";
        private static string REMOVE_DESCRIPTION = "calls to remove";

        private static string REMOVE_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + REMOVE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string REMOVE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + REMOVE_DESCRIPTION;
        private static string REMOVE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + REMOVE_DESCRIPTION;

        private static string RESTORE = "restore";
        private static string RESTORE_DESCRIPTION = "restorations";

        private static string RESTORE_RATE_DESCRIPTION =
            RATE_DESCRIPTION_PREFIX + RESTORE_DESCRIPTION + RATE_DESCRIPTION_SUFFIX;

        private static string RESTORE_AVG_LATENCY_DESCRIPTION = AVG_LATENCY_DESCRIPTION_PREFIX + RESTORE_DESCRIPTION;
        private static string RESTORE_MAX_LATENCY_DESCRIPTION = MAX_LATENCY_DESCRIPTION_PREFIX + RESTORE_DESCRIPTION;

        private static string SUPPRESSION_BUFFER_COUNT = "suppression-buffer-count";
        private static string SUPPRESSION_BUFFER_COUNT_DESCRIPTION = "count of " + BUFFERED_RECORDS;

        private static string SUPPRESSION_BUFFER_COUNT_AVG_DESCRIPTION =
            AVG_DESCRIPTION_PREFIX + SUPPRESSION_BUFFER_COUNT_DESCRIPTION;

        private static string SUPPRESSION_BUFFER_COUNT_MAX_DESCRIPTION =
            MAX_DESCRIPTION_PREFIX + SUPPRESSION_BUFFER_COUNT_DESCRIPTION;

        private static string SUPPRESSION_BUFFER_SIZE = "suppression-buffer-size";
        private static string SUPPRESSION_BUFFER_SIZE_DESCRIPTION = "size of " + BUFFERED_RECORDS;

        private static string SUPPRESSION_BUFFER_SIZE_AVG_DESCRIPTION =
            AVG_DESCRIPTION_PREFIX + SUPPRESSION_BUFFER_SIZE_DESCRIPTION;

        private static string SUPPRESSION_BUFFER_SIZE_MAX_DESCRIPTION =
            MAX_DESCRIPTION_PREFIX + SUPPRESSION_BUFFER_SIZE_DESCRIPTION;

        private static string EXPIRED_WINDOW_RECORD_DROP = "expired-window-record-drop";
        private static string EXPIRED_WINDOW_RECORD_DROP_DESCRIPTION = "dropped records due to an expired window";

        private static string EXPIRED_WINDOW_RECORD_DROP_TOTAL_DESCRIPTION =
            StreamMetricsRegistry.TOTAL_DESCRIPTION + EXPIRED_WINDOW_RECORD_DROP_DESCRIPTION;

        private static string EXPIRED_WINDOW_RECORD_DROP_RATE_DESCRIPTION =
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
                streamsMetrics.StoreLevelTags(GetThreadId(), taskId.ToString(), storeType, storeName),
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
            tags = streamsMetrics.StoreLevelTags(GetThreadId(), taskId.ToString(), storeType, storeName);

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
                streamsMetrics.StoreLevelTags(GetThreadId(), taskId.ToString(), storeType, storeName);

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