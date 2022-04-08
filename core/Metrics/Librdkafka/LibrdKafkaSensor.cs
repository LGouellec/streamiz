using System;
using System.Collections.Generic;
using System.Linq;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Stats;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    /// <summary>
    /// Dynamic sensor functions topics, partitions, brokers ...
    /// </summary>
    internal class LibrdKafkaSensor : Sensor
    {
        internal class ScopedLibrdKafka : IEquatable<ScopedLibrdKafka>
        {
            internal static readonly string internalSeparator = ":";
            internal static readonly string separator = "#";
            
            public String Name { get; set; }
            public string Scope { get; set; }
            public long LastRecordedTime { get; set; } = -1;

            public bool Equals(ScopedLibrdKafka other)
            {
                if (other == null)
                    return false;
                return other.Name.Equals(Name) && other.Scope.Equals(Scope);
            }
        }

        internal class ScopedLibrdKafkaSensor : Sensor
        {
            internal ScopedLibrdKafka scoped;
            
            internal ScopedLibrdKafkaSensor(LibrdKafkaSensor sensor, ScopedLibrdKafka scoped) 
                : base(sensor.Name, sensor.Description, sensor.MetricsRecording)
            {
                this.scoped = scoped;

                foreach (var m in sensor.originMeasurableStats)
                {
                    var cloneMeasurableStat = (IMeasurableStat)Activator.CreateInstance(m.Value.GetType());
                    var tags = new Dictionary<string, string>(m.Key.Tags);
                    scoped.Scope.Split(ScopedLibrdKafka.separator)
                        .ForEach((i) =>
                        {
                            var items = i.Split(ScopedLibrdKafka.internalSeparator);
                            if(items.Length == 2)
                                tags.Add(items[0], items[1]);
                        });

                    var newKeyMetricName = new MetricName(
                        $"{m.Key.Name}-{scoped.Scope}",
                        m.Key.Group,
                        m.Key.Description,
                        tags
                    );
                    
                    var newMetricName = new MetricName(
                        m.Key.Name,
                        m.Key.Group,
                        m.Key.Description,
                        tags
                    );
                    
                    AddStatMetric(newKeyMetricName, newMetricName, cloneMeasurableStat);
                }
            }
            
            internal override void Record()
            {
                base.Record();
                scoped.LastRecordedTime = DateTime.Now.GetMilliseconds();
            }

            internal override void Record(long value)
            {
                base.Record(value);
                scoped.LastRecordedTime = DateTime.Now.GetMilliseconds();
            }

            internal override void Record(double value, long timeMs)
            {
                base.Record(value, timeMs);
                scoped.LastRecordedTime = DateTime.Now.GetMilliseconds();
            }
        }

        private readonly Dictionary<MetricName, StreamMetric> originMetrics;
        private readonly Dictionary<MetricName, IMeasurableStat> originMeasurableStats;
        private readonly Dictionary<ScopedLibrdKafka, ScopedLibrdKafkaSensor> scopedLibrdKafkaSensors;
        private readonly MetricConfig config = new MetricConfig();

        public override IReadOnlyDictionary<MetricName, StreamMetric> Metrics
        {
            get
            {
                if (scopedLibrdKafkaSensors.Count == 0)
                    return base.Metrics;
                else
                {
                  return scopedLibrdKafkaSensors
                        .SelectMany(s => s.Value.Metrics)
                        .ToDictionary(k =>
                            new MetricName(k.Key.Name, k.Value.Group, k.Value.Description,
                                k.Value.Tags.ToDictionary()),
                                k => k.Value);
                }
            }
        }

        internal LibrdKafkaSensor(string name, string description, MetricsRecordingLevel metricsRecording)
            : base(name, description, metricsRecording)
        {
            scopedLibrdKafkaSensors = new Dictionary<ScopedLibrdKafka, ScopedLibrdKafkaSensor>();
        }
        
        internal ScopedLibrdKafkaSensor Scoped(params (string, string)[] scopedTags)
        {
            ScopedLibrdKafka scoped = new ScopedLibrdKafka()
            {
                Name = this.Name,
                Scope = string.Join(ScopedLibrdKafka.separator,
                    scopedTags.Select(i => $"{i.Item1}{ScopedLibrdKafka.internalSeparator}{i.Item2}"))
            };
            
            if (scopedLibrdKafkaSensors.ContainsKey(scoped))
                return scopedLibrdKafkaSensors[scoped];
            else
            {
                var scopedSensor = new ScopedLibrdKafkaSensor(this, scoped);
                scopedLibrdKafkaSensors.Add(scoped, scopedSensor);
                return scopedSensor;
            }
        }

        internal void RemoveOldScopeSensor(long now)
        {
            List<ScopedLibrdKafka> toRemove = new List<ScopedLibrdKafka>();
            
            foreach (var scopedSensor in scopedLibrdKafkaSensors)
            {
                if (scopedSensor.Key.LastRecordedTime < now)
                    toRemove.Add(scopedSensor.Key);
            }
            
            scopedLibrdKafkaSensors.RemoveAll(toRemove);
        }
        
        internal override bool AddStatMetric(MetricName name, IMeasurableStat stat, MetricConfig config = null)
        {
            if(!originMetrics.ContainsKey(name))
            {
                StreamMetric metric = new StreamMetric(name, stat, config ?? this.config);
                originMetrics.Add(name, metric);
                originMeasurableStats.Add(name, stat);
                return true;
            }
            
            return false;        
        }
    }
}