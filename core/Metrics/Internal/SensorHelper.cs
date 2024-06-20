using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal static class SensorHelper
    {
        #region RateAndCount
        
        internal static Sensor InvocationRateAndCountSensor(string threadId,
                         string metricName,
                         string metricDescription,
                         string descriptionOfRate,
                         string descriptionOfCount,
                         MetricsRecordingLevel recordingLevel,
                         StreamMetricsRegistry streamsMetrics) {
            
            Sensor sensor = streamsMetrics.ThreadLevelSensor(threadId, metricName, metricDescription, recordingLevel);
            
            AddInvocationRateAndCountToSensor(
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

            AddAvgAndMaxToSensor(sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                tags,
                metricName + StreamMetricsRegistry.LATENCY_SUFFIX,
                descriptionOfAvg,
                descriptionOfMax);

            AddInvocationRateAndCountToSensor(sensor,
                StreamMetricsRegistry.THREAD_LEVEL_GROUP,
                tags,
                metricName,
                descriptionOfRate,
                descriptionOfCount);
            
            return sensor;
        }
        
        #endregion
        
        internal static void AddInvocationRateAndCountToSensor(Sensor sensor,
            string group,
            IDictionary<string, string> tags,
            string operation,
            string descriptionOfRate,
            string descriptionOfCount) {
            
            AddInvocationRateToSensor(sensor, group, tags, operation, descriptionOfRate);

            sensor.AddStatMetric(
                new MetricName(
                    operation + StreamMetricsRegistry.TOTAL_SUFFIX,
                    group,
                    descriptionOfCount,
                    tags
                ), new CumulativeCount());
        }
        
        internal static void AddInvocationRateToSensor(Sensor sensor,
            string group,
            IDictionary<string, string> tags,
            string operation,
            string descriptionOfRate) {
            
            sensor.AddStatMetric(
                new MetricName(
                    operation + StreamMetricsRegistry.RATE_SUFFIX,
                    group,
                    descriptionOfRate,
                    tags
                ),
                new Rate(TimeUnits.SECONDS, new WindowedCount())
            );
        }

        internal static void AddAvgAndMaxToSensor(Sensor sensor,
            string group,
            IDictionary<string, string> tags,
            string operation,
            string descriptionOfAvg,
            string descriptionOfMax) {
            
            sensor.AddStatMetric(
                new MetricName(
                    operation + StreamMetricsRegistry.AVG_SUFFIX,
                    group,
                    descriptionOfAvg,
                    tags),
                new Avg()
            );
            sensor.AddStatMetric(
                new MetricName(
                    operation + StreamMetricsRegistry.MAX_SUFFIX,
                    group,
                    descriptionOfMax,
                    tags),
                new Max()
            );
        }
        
        internal static void AddAvgAndMinAndMaxToSensor(Sensor sensor,
            string group,
            IDictionary<string, string> tags,
            string operation,
            string descriptionOfAvg,
            string descriptionOfMax,
            string descriptionOfMin) {
            
            sensor.AddStatMetric(
                new MetricName(
                    operation + StreamMetricsRegistry.AVG_SUFFIX,
                    group,
                    descriptionOfAvg,
                    tags),
                new Avg()
            );
            sensor.AddStatMetric(
                new MetricName(
                    operation + StreamMetricsRegistry.MAX_SUFFIX,
                    group,
                    descriptionOfMax,
                    tags),
                new Max()
            );
            sensor.AddStatMetric(
                new MetricName(
                    operation + StreamMetricsRegistry.MIN_SUFFIX,
                    group,
                    descriptionOfMin,
                    tags),
                new Min()
            );
        }
        
        internal static void AddRateOfSumAndSumMetricsToSensor(Sensor sensor,
            string group,
            IDictionary<string, string> tags,
            string operation,
            string descriptionOfRate,
            string descriptionOfTotal) {
            
            AddRateOfSumMetricToSensor(sensor, group, tags, operation, descriptionOfRate);
            AddSumMetricToSensor(sensor, group, tags, operation, descriptionOfTotal);
        }

        internal static void AddRateOfSumMetricToSensor(Sensor sensor,
            string group,
            IDictionary<string, string> tags,
            string operation,
            string description)
        {
            sensor.AddStatMetric(
                new MetricName(
                    operation + StreamMetricsRegistry.RATE_SUFFIX,
                    group,
                    description,
                    tags), new Rate(TimeUnits.SECONDS, new WindowedSum()));
        }

        internal static void AddSumMetricToSensor(Sensor sensor,
            string group,
            IDictionary<string, string> tags,
            string operation,
            string description)
            => AddSumMetricToSensor(sensor, group, tags, operation, description, true);
        
        internal static void AddSumMetricToSensor(Sensor sensor,
            string group,
            IDictionary<string, string> tags,
            string operation,
            string description,
            bool withSuffix)
        {
            sensor.AddStatMetric(
                new MetricName(
                    withSuffix ? operation + StreamMetricsRegistry.TOTAL_SUFFIX : operation,
                    group,
                    description, tags),
                new CumulativeSum());
        }
        
        internal static void AddValueMetricToSensor(Sensor sensor,
             string group,
             IDictionary<string, string> tags,
             string name,
             string description) 
        {
            sensor.AddStatMetric(new MetricName(name, group, description, tags), new Value());
        }
    }
}