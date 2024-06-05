using System;
using System.Collections.Generic;
using System.Threading;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class CachingMetrics
    {
        internal static string CACHE_SIZE_BYTES_TOTAL = "cache-size-bytes-total";
        private static string CACHE_SIZE_BYTES_TOTAL_DESCRIPTION = "The total size in bytes of this cache state store.";
        
        internal static string HIT_RATIO = "hit-ratio";
        private static string HIT_RATIO_DESCRIPTION = "The hit ratio defined as the ratio of cache read hits over the total cache read requests.";
        private static string HIT_RATIO_AVG_DESCRIPTION = "The average cache hit ratio";
        private static string HIT_RATIO_MIN_DESCRIPTION = "The minimum cache hit ratio";
        private static string HIT_RATIO_MAX_DESCRIPTION = "The maximum cache hit ratio";
        
        public static Sensor HitRatioSensor(
            TaskId taskId, 
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics) {

            Sensor sensor;
            string hitMetricName = HIT_RATIO;
            IDictionary<string, string> tags =
                streamsMetrics.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            sensor = streamsMetrics.StoreLevelSensor(GetThreadId(), taskId, storeName, hitMetricName, HIT_RATIO_DESCRIPTION, MetricsRecordingLevel.DEBUG);
            
            SensorHelper.AddAvgAndMinAndMaxToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                hitMetricName,
                HIT_RATIO_AVG_DESCRIPTION,
                HIT_RATIO_MAX_DESCRIPTION,
                HIT_RATIO_MIN_DESCRIPTION);
            
            return sensor;
        }
        
        public static Sensor TotalCacheSizeBytesSensor(
            TaskId taskId, 
            string storeType,
            string storeName,
            StreamMetricsRegistry streamsMetrics) {

            Sensor sensor;
            string totalCacheMetricName = CACHE_SIZE_BYTES_TOTAL;
            IDictionary<string, string> tags =
                streamsMetrics.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            sensor = streamsMetrics.StoreLevelSensor(GetThreadId(), taskId, storeName, totalCacheMetricName, CACHE_SIZE_BYTES_TOTAL_DESCRIPTION, MetricsRecordingLevel.DEBUG);
            
            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                totalCacheMetricName,
                CACHE_SIZE_BYTES_TOTAL_DESCRIPTION);
            
            return sensor;
        }
        
        private static string GetThreadId() => Thread.CurrentThread.Name;
    }
}