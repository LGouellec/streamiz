#nullable enable
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Metrics
{
    /// <summary>
    ///  A sensor applies a continuous sequence of numerical values to a set of associated metrics. For example a sensor on
    /// message size would record a sequence of message sizes using the <see cref="Record()"/> api and would maintain a set
    /// of metrics about request sizes such as the average or max.
    /// </summary>
    public class Sensor : IEquatable<Sensor>, IComparable<Sensor>
    {
        private readonly Dictionary<MetricName, StreamMetric> metrics;
        private readonly IList<IMeasurableStat> stats;
        private MetricConfig config = new MetricConfig();
        
        /// <summary>
        /// Lock object to synchronize recording
        /// </summary>
        protected readonly object @lock = new object();

        /// <summary>
        /// True if the sensor is not runnable because the metris recording level is not compatible with this one passed on configuration
        /// </summary>
        internal bool NoRunnable { get; set; } = false;
        
        /// <summary>
        /// The name this sensor is registered with. This name will be unique among all registered sensors.
        /// </summary>
        public string Name { get; private set; }
        
        /// <summary>
        /// A human-readable description to include in the sensor.
        /// </summary>
        public string Description { get; private set; }
        
        /// <summary>
        /// Define the metrics recording level of this sensor
        /// </summary>
        public MetricsRecordingLevel MetricsRecording { get; private set; }
        
        /// <summary>
        /// Expose all metrics containing the sensor
        /// </summary>
        public virtual IReadOnlyDictionary<MetricName, StreamMetric> Metrics =>
            new ReadOnlyDictionary<MetricName, StreamMetric>(metrics);

        /// <summary>
        /// Only one constructor, really important (Reflection use <see cref="StreamMetricsRegistry.GetSensor{T}(System.Collections.Generic.IDictionary{string,System.Collections.Generic.IList{string}},string,string,string,Streamiz.Kafka.Net.Metrics.MetricsRecordingLevel,Streamiz.Kafka.Net.Metrics.Sensor[])"/>)
        /// </summary>
        /// <param name="name"></param>
        /// <param name="description"></param>
        /// <param name="metricsRecording"></param>
        internal Sensor(
            string name,
            string description,
            MetricsRecordingLevel metricsRecording)
        {
            Name = name;
            Description = description;
            MetricsRecording = metricsRecording;
            metrics = new Dictionary<MetricName, StreamMetric>();
            stats = new List<IMeasurableStat>();
            config.SetRecordingLevel(MetricsRecording);
        }
        
        #region Add

        internal bool AddStatMetric(MetricName keyMetricName, MetricName metricName, IMeasurableStat stat, MetricConfig config = null)
        {
            if (!NoRunnable)
            {
                if (!metrics.ContainsKey(keyMetricName))
                {
                    StreamMetric metric = new StreamMetric(metricName, stat, config ?? this.config);
                    metrics.Add(keyMetricName, metric);
                    stats.Add(stat);
                    return true;
                }

                return false;
            }

            return false;
        }

        internal virtual bool AddStatMetric(MetricName name, IMeasurableStat stat, MetricConfig config = null)
        {
            if (!NoRunnable)
            {
                if (!metrics.ContainsKey(name))
                {
                    StreamMetric metric = new StreamMetric(name, stat, config ?? this.config);
                    metrics.Add(name, metric);
                    stats.Add(stat);
                    return true;
                }

                return false;
            }

            return false;
        }

        internal virtual bool AddImmutableMetric<T>(MetricName name, T value, MetricConfig config = null)
        {
            if (!NoRunnable)
            {
                if (!metrics.ContainsKey(name))
                {
                    StreamMetric metric =
                        new StreamMetric(name, new ImmutableMetricValue<T>(value), config ?? this.config);
                    metrics.Add(name, metric);
                    if (value is IMeasurableStat stat)
                        stats.Add(stat);
                    return true;
                }

                return false;
            }

            return false;
        }
        
        internal virtual bool AddProviderMetric<T>(MetricName name, Func<T> provider, MetricConfig config = null)
        {
            if (!NoRunnable)
            {
                if (!metrics.ContainsKey(name))
                {
                    StreamMetric metric = new StreamMetric(name, new ProviderMetricValue<T>(provider),
                        config ?? this.config);
                    metrics.Add(name, metric);
                    return true;
                }

                return false;
            }

            return false;
        }

        #endregion

        #region Record

        internal void Record() 
            => Record(1);
        
        internal void Record(long value)
            => Record(value, DateTime.Now.GetMilliseconds());
        
        internal void Record(double value)
            => Record(value, DateTime.Now.GetMilliseconds());
        
        internal virtual void Record(double value, long timeMs)
            => RecordInternal(value, timeMs);
        
        /// <summary>
        /// Record <paramref name="value"/> in every metrics
        /// </summary>
        /// <param name="value">New value</param>
        /// <param name="timeMs">Time in milliseconds</param>
        protected void RecordInternal(double value, long timeMs)
        {
            if (!NoRunnable)
            {
                lock (@lock)
                {
                    foreach (var stat in stats)
                        stat.Record(config, value, timeMs);
                }
            }
        }
        
        #endregion

        internal Sensor ChangeTagValue(string tag, string newValue)
        {
            if (!NoRunnable)
            {
                foreach (var metric in metrics)
                {
                    metric.Key.Tags.AddOrUpdate(tag, newValue);
                    metric.Value.ChangeTagValue(tag, newValue);
                }
            }

            return this;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool Equals(Sensor? other)
            => other != null && other.Name.Equals(Name);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public int CompareTo(Sensor? other)
            => other != null ? other.Name.CompareTo(Name) : 1;
        
    }
}