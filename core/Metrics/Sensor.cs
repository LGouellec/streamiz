using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Metrics
{
    public class Sensor : IEquatable<Sensor>, IComparable<Sensor>
    {
        internal readonly Dictionary<MetricName, StreamMetric> metrics;
        internal readonly IList<IMeasurableStat> stats;
        internal MetricConfig config = new MetricConfig();
        
        private readonly object @lock = new object();

        /// <summary>
        /// True if the sensor is not runnable because the metris recording level is not compatible with this one passed on configuration
        /// </summary>
        internal bool NoRunnable { get; set; } = false;
        public string Name { get; private set; }
        public string Description { get; private set; }
        public MetricsRecordingLevel MetricsRecording { get; private set; }
        public virtual IReadOnlyDictionary<MetricName, StreamMetric> Metrics =>
            new ReadOnlyDictionary<MetricName, StreamMetric>(metrics);

        // Only one constructor, really important
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
                    if (value is IMeasurableStat)
                        stats.Add((IMeasurableStat) value);
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

        internal virtual void Record() 
            => Record(1);
        
        internal virtual void Record(long value)
            => Record(value, DateTime.Now.GetMilliseconds());
        
        internal virtual void Record(double value, long timeMs)
            => RecordInternal(value, timeMs);
        
        protected virtual void RecordInternal(double value, long timeMs)
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

        internal virtual void Refresh(long now)
        {
            if (!NoRunnable)
            {
                lock (@lock)
                {
                    foreach (var metric in metrics.Values)
                        metric.Measure(now);
                }
            }
        }

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
        
        public bool Equals(Sensor? other)
            => other != null && other.Name.Equals(Name);

        public int CompareTo(Sensor? other)
            => other != null ? other.Name.CompareTo(Name) : 1;
        
    }
}