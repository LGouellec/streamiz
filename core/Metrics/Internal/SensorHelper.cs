using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal static class SensorHelper
    {
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