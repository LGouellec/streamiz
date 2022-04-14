namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    internal interface IStatisticsHandler
    {
        void Register(StreamMetricsRegistry metricsRegistry);
        void Publish(Statistics statistics);
        // TODO : to remove
        void Unregister(StreamMetricsRegistry metricsRegistry);
    }
}