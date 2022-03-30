namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    internal interface IStatisticsHandler
    {
        void Register(StreamMetricsRegistry metricsRegistry);
        void Publish(Statistics statistics);
        void Unregister(StreamMetricsRegistry metricsRegistry);
    }
}