namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal class CumulativeSum : IMeasurableStat
    {
        private double total;

        public CumulativeSum() : this(0.0)
        { }
        
        public CumulativeSum(double total)
        {
            this.total = total;
        }
        
        public virtual void Record(MetricConfig config, double value, long timeMs)
        {
            total += value;
        }

        public double Measure(MetricConfig config, long now)
            => total;
    }
}