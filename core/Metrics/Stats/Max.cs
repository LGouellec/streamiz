using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal class Max : SampledStat
    {
        public Max() 
            : base(Double.NegativeInfinity)
        {
        }

        protected override void Update(Sample sample, MetricConfig config, double value, long timeMs)
        {
            sample.Value = Math.Max(sample.Value, value);
        }

        protected override double Combine(List<Sample> samples, MetricConfig config, long now)
        {
            double max = Double.NegativeInfinity;
            long count = 0;
            foreach (var s in samples)
            {
                max = Math.Max(max, s.Value);
                count += s.EventCount;
            }

            return count == 0 ? Double.NaN : max;
        }
    }
}