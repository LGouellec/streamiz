using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal class Avg : SampledStat
    {
        public Avg() 
            : base(0.0)
        {
        }

        protected override void Update(Sample sample, MetricConfig config, double value, long timeMs)
        {
            sample.Value += value;
        }

        protected override double Combine(List<Sample> samples, MetricConfig config, long now)
        {
            double total = 0.0;
            long count = 0;
            foreach (var s in samples)
            {
                total += s.Value;
                count += s.EventCount;
            }

            return count == 0 ? Double.NaN : total / count;
        }
    }
}