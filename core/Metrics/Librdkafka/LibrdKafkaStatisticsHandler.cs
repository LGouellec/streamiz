using System;
using System.Collections.Generic;
using Microsoft.VisualBasic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Internal;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    internal abstract class LibrdKafkaStatisticsHandler : IStatisticsHandler
    {
        protected readonly string clientId;
        protected readonly string streamAppId;
        protected readonly string threadId;
        
        // Per Topic (add topic name as label)			
        protected LibrdKafkaSensor BatchSizeAverageBytesSensor;
        protected LibrdKafkaSensor BatchMessageCountsAverageSensor;

        protected LibrdKafkaStatisticsHandler(
            string clientId,
            string streamAppId,
            string threadId)
        {
            this.clientId = clientId;
            this.streamAppId = streamAppId;
            this.threadId = threadId ?? StreamMetricsRegistry.UNKNOWN_THREAD;
        }

        public abstract void Register(StreamMetricsRegistry metricsRegistry);

        public abstract void Publish(Statistics statistics);

        protected void PublishTopicsStatistics(Dictionary<string, TopicStatistic> statisticsTopics)
        {
            long now = DateTime.Now.GetMilliseconds();
            foreach (var topic in statisticsTopics)
            {
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(BatchSizeAverageBytesSensor
                        .Scoped((LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName))
                    , topic.Value.BatchSize.Average, now);

                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(BatchMessageCountsAverageSensor
                        .Scoped((LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName))
                    , topic.Value.BatchMessageCounts.Average, now);
            }
        }
    }
}