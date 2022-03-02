using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal class Min : SampledStat
    {
        public Min() 
            : base(Double.MaxValue)
        {
        }

        protected override void Update(Sample sample, MetricConfig config, double value, long timeMs)
        {
            sample.Value = Math.Min(sample.Value, value);
        }

        protected override double Combine(List<Sample> samples, MetricConfig config, long now)
        {
            double min = Double.MaxValue;
            long count = 0;
            foreach (var s in samples)
            {
                min = Math.Min(min, s.Value);
                count += s.EventCount;
            }

            return count == 0 ? Double.NaN : min;
        }
    }
}