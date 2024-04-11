using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Internal;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    internal class ConsumerStatisticsHandler : LibrdKafkaStatisticsHandler
    {
        private readonly bool isGlobalConsumer;
        
        private Sensor TotalNumberOfMessagesConsumedSensor;
        private Sensor TotalNumberOfMessageBytesConsumedSensor;
        private Sensor NumberOfOpsWaitinInQueueSensor; // Sensor
        private Sensor TotalNumberOfResponsesReceivedFromKafkaSensor;
        private Sensor TotalNumberOfBytesReceivedFromKafkaSensor;
        private Sensor RebalanceAgeSensor; // Sensor
        private Sensor TotalNumberOfRelabalanceSensor; // Sensor assign or revoke

        //PER BROKER (add Broker NodeId as label)
        private LibrdKafkaSensor TotalNumberOfResponsesReceivedSensor;
        private LibrdKafkaSensor TotalNumberOfBytesReceivedSensor;
        private LibrdKafkaSensor TotalNumberOfReceivedErrorsSensor;
        private LibrdKafkaSensor NumberOfConnectionAttempsSensor; // Including successful, failed and name resolution failures
        private LibrdKafkaSensor NumberOfDisconnectsSensor;
        private LibrdKafkaSensor BrokerLatencyAverageMsSensor;

        // Per partition(topic brokder id PartitionId as label)

        private LibrdKafkaSensor ConsumerLagSensor; // Sensor
        private LibrdKafkaSensor TotalNumberOfMessagesConsumedByPartitionSensor; // Sensor
        private LibrdKafkaSensor TotalNumberOfBytesConsumedByPartitionSensor; // Sensor

        public ConsumerStatisticsHandler(
            string clientId,
            string streamAppId,
            string threadId = null, 
            bool isGlobalConsumer = false) 
            : base(clientId, streamAppId, threadId)
        {
            this.isGlobalConsumer = isGlobalConsumer;
        }
        

        public override void Register(StreamMetricsRegistry metricsRegistry)
        {
            TotalNumberOfMessagesConsumedSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfMessagesConsumedSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            TotalNumberOfMessageBytesConsumedSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfMessageBytesConsumedSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            NumberOfOpsWaitinInQueueSensor =
                LibrdKafkaConsumerMetrics.NumberOfOpsWaitinInQueueSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            TotalNumberOfResponsesReceivedFromKafkaSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfResponsesReceivedFromKafkaSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            TotalNumberOfBytesReceivedFromKafkaSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfBytesReceivedFromKafkaSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            RebalanceAgeSensor =
                LibrdKafkaConsumerMetrics.RebalanceAgeSensor(threadId, clientId, streamAppId, metricsRegistry);
            TotalNumberOfRelabalanceSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfRelabalanceSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            TotalNumberOfResponsesReceivedSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfResponsesReceivedSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            TotalNumberOfBytesReceivedSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfBytesReceivedSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            TotalNumberOfReceivedErrorsSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfReceivedErrorsSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            NumberOfConnectionAttempsSensor =
                LibrdKafkaConsumerMetrics.NumberOfConnectionAttempsSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            NumberOfDisconnectsSensor =
                LibrdKafkaConsumerMetrics.NumberOfDisconnectsSensor(threadId, clientId, streamAppId, metricsRegistry);
            BrokerLatencyAverageMsSensor =
                LibrdKafkaConsumerMetrics.BrokerLatencyAverageMsSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            BatchSizeAverageBytesSensor =
                LibrdKafkaConsumerMetrics.BatchSizeAverageBytesSensor(threadId, clientId, streamAppId, metricsRegistry);
            BatchMessageCountsAverageSensor =
                LibrdKafkaConsumerMetrics.BatchMessageCountsAverageSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
            ConsumerLagSensor =
                LibrdKafkaConsumerMetrics.ConsumerLagSensor(threadId, clientId, streamAppId, metricsRegistry);
            TotalNumberOfMessagesConsumedByPartitionSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfMessagesConsumedByPartitionSensor(threadId, clientId,
                    streamAppId, metricsRegistry);
            TotalNumberOfBytesConsumedByPartitionSensor =
                LibrdKafkaConsumerMetrics.TotalNumberOfBytesConsumedByPartitionSensor(threadId, clientId, streamAppId,
                    metricsRegistry);
        }

        public override void Publish(Statistics statistics)
        {
            TotalNumberOfMessagesConsumedSensor.Record(statistics.TotalNumberOfMessagesConsumed);
            TotalNumberOfMessageBytesConsumedSensor.Record(statistics.TotalNumberOfMessageBytesConsumed);
            NumberOfOpsWaitinInQueueSensor.Record(statistics.NumberOfOpsWaitinInQueue);
            TotalNumberOfResponsesReceivedFromKafkaSensor.Record(statistics.TotalNumberOfResponsesReceivedFromKafka);
            TotalNumberOfBytesReceivedFromKafkaSensor.Record(statistics.TotalNumberOfBytesReceivedFromKafka);
            RebalanceAgeSensor.Record(statistics.ConsumerGroups.RebalanceAge);
            TotalNumberOfRelabalanceSensor.Record(statistics.ConsumerGroups.TotalNumberOfRelabalance);

            PublishBrokerStats(statistics.Brokers);
            PublishTopicsStats(statistics.Topics);
        }
        
        private void PublishTopicsStats(Dictionary<string, TopicStatistic> statisticsTopics)
        {
            long now = DateTime.Now.GetMilliseconds();

            PublishTopicsStatistics(statisticsTopics);
            
            foreach (var topic in statisticsTopics)
            {
                foreach (var partition in topic.Value.Partitions)
                {
                    LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(ConsumerLagSensor
                        .Scoped(
                            (LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName),
                            (LibrdKafkaBaseMetrics.BROKER_ID_TAG, partition.Value.BrokerId.ToString()), 
                            (LibrdKafkaBaseMetrics.PARTITION_ID_TAG, partition.Value.PartitionId.ToString()))
                        , !isGlobalConsumer ? partition.Value.ConsumerLag : partition.Value.ConsumerLagStored,
                        now);

                    LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(TotalNumberOfMessagesConsumedByPartitionSensor
                        .Scoped(
                            (LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName),
                            (LibrdKafkaBaseMetrics.BROKER_ID_TAG, partition.Value.BrokerId.ToString()), 
                            (LibrdKafkaBaseMetrics.PARTITION_ID_TAG, partition.Value.PartitionId.ToString()))
                        , partition.Value.TotalNumberOfMessagesconsumed, now);

                    LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(TotalNumberOfBytesConsumedByPartitionSensor
                        .Scoped(
                            (LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName),
                            (LibrdKafkaBaseMetrics.BROKER_ID_TAG, partition.Value.BrokerId.ToString()), 
                            (LibrdKafkaBaseMetrics.PARTITION_ID_TAG, partition.Value.PartitionId.ToString()))
                        , partition.Value.TotalNumberOfBytesConsumed, now);
                }
            }

            BatchSizeAverageBytesSensor.RemoveOldScopeSensor(now);
            BatchMessageCountsAverageSensor.RemoveOldScopeSensor(now);
            ConsumerLagSensor.RemoveOldScopeSensor(now);
            TotalNumberOfMessagesConsumedByPartitionSensor.RemoveOldScopeSensor(now);
            TotalNumberOfBytesConsumedByPartitionSensor.RemoveOldScopeSensor(now);
        }

        private void PublishBrokerStats(Dictionary<string, BrokerStatistic> statisticsBrokers)
        {
            long now = DateTime.Now.GetMilliseconds();

            foreach (var broker in statisticsBrokers)
            {
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(TotalNumberOfResponsesReceivedSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.TotalNumberOfResponsesReceived, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(TotalNumberOfBytesReceivedSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.TotalNumberOfBytesReceived, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(TotalNumberOfReceivedErrorsSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.TotalNumberOfReceivedErrors, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(NumberOfConnectionAttempsSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.NumberOfConnectionAttemps, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(NumberOfDisconnectsSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.NumberOfDisconnects, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(BrokerLatencyAverageMsSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.BrokerLatency.Average, now);
            }

            TotalNumberOfResponsesReceivedSensor.RemoveOldScopeSensor(now);
            TotalNumberOfBytesReceivedSensor.RemoveOldScopeSensor(now);
            TotalNumberOfReceivedErrorsSensor.RemoveOldScopeSensor(now);
            NumberOfConnectionAttempsSensor.RemoveOldScopeSensor(now);
            NumberOfDisconnectsSensor.RemoveOldScopeSensor(now);
            BrokerLatencyAverageMsSensor.RemoveOldScopeSensor(now);
        }
    }
}