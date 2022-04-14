using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal class WindowedSum : SampledStat
    {
        public WindowedSum() 
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
            foreach (var sample in samples)
                total += sample.Value;
            return total;
        }
    }
}