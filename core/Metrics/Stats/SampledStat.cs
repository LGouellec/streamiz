using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics.Stats
{
    /// <summary>
    /// A SampledStat records a single scalar value measured over one or more samples. Each sample is recorded over a
    /// configurable window. The window can be defined by number of events or elapsed time (or both, if both are given the
    /// window is complete when either the event count or elapsed time criterion is met).
    /// <para>
    /// All the samples are combined to produce the measurement. When a window is complete the oldest sample is cleared and
    /// recycled to begin recording the next sample.
    /// </para>
    /// Subclasses of this class define different statistics measured using this basic pattern.
    /// </summary>
    internal abstract class SampledStat : IMeasurableStat
    {
        internal class Sample
        {
            public Sample(double initValue, long timeMs)
            {
                InitialValue = initValue;
                EventCount = 0;
                LastWindowMs = timeMs;
                Value = initValue;
            }

            public double InitialValue { get; }
            public long EventCount { get; set; }
            public long LastWindowMs { get; set; }
            public double Value { get; set; }

            public void Reset(long now)
            {
                EventCount = 0;
                LastWindowMs = now;
                Value = InitialValue;
            }
            
            public bool IsComplete(long timeMs, MetricConfig config) 
            {
                return timeMs - LastWindowMs >= config.TimeWindowMs || EventCount >= config.EventWindow;
            }
        }

        private double initValue = 0;
        private int current = 0;
        private List<Sample> samples;

        protected SampledStat(double initValue)
        {
            this.initValue = initValue;
            samples = new List<Sample>(2);
        }
        
        protected Sample NewSample(long timeMs) => new Sample(initValue, timeMs);
        internal void PurgeObsoleteSamples(MetricConfig config, long now) {
            long expireAge = config.Samples * config.TimeWindowMs;
            foreach (Sample sample in samples) {
                if (now - sample.LastWindowMs >= expireAge)
                    sample.Reset(now);
            }
        }
        private Sample Advance(MetricConfig config, long timeMs)
        {
            current = (current + 1) % config.Samples;
            if (current >= samples.Count)
            {
                Sample sample = NewSample(timeMs);
                samples.Add(sample);
                return sample;
            }
            else
            {
                Sample sample = Current(timeMs);
                sample.Reset(timeMs);
                return sample;
            }
        }
        public Sample Current(long now)
        {
            if (samples.Count == 0)
                samples.Add(NewSample(now));
            return samples[current];
        }
        
        public Sample Oldest(long now)
        {
            if (samples.Count == 0)
                samples.Add(NewSample(now));
            Sample oldest = samples[0];
            for (int i = 1; i < samples.Count; i++)
            {
                Sample curr = samples[i];
                if (curr.LastWindowMs < oldest.LastWindowMs)
                    oldest = curr;
            }

            return oldest;
        }
        protected abstract void Update(Sample sample, MetricConfig config, double value, long timeMs);
        protected abstract double Combine(List<Sample> samples, MetricConfig config, long now);

        public void Record(MetricConfig config, double value, long timeMs)
        {
            Sample sample = Current(timeMs);
            if (sample.IsComplete(timeMs, config))
                sample = Advance(config, timeMs);
            Update(sample, config, value, timeMs);
            sample.EventCount += 1;
        }

        public double Measure(MetricConfig config, long now)
        {
            PurgeObsoleteSamples(config, now);
            return Combine(samples, config, now);
        }
    }
}