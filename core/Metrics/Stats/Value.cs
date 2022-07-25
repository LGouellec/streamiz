namespace Streamiz.Kafka.Net.Metrics.Stats
{
    internal class Value : IMeasurableStat
    {
        private double value = 0.0;
        
        public void Record(MetricConfig config, double value, long timeMs)
        {
            this.value = value;
        }

        public double Measure(MetricConfig config, long now)
            => value;
    }
}