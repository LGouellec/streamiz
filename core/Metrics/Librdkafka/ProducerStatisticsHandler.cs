using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics.Internal;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
    internal class ProducerStatisticsHandler : IStatisticsHandler
    {
        private readonly string producerId;
        private readonly string threadId;
        private readonly string streamAppId;
        private readonly string taskId;

        private Sensor TotalNumberOfMessagesProducedSensor;
        private Sensor TotalNumberOfMessageBytesProducedSensor;
        private Sensor NumberOfOpsWaitinInQueueSensor;
        private Sensor CurrentNumberOfMessagesInProducerQueuesSensor;
        private Sensor CurrentSizeOfMessagesInProducerQueuesSensor;
        private Sensor MaxMessagesAllowedOnProducerQueuesSensor;
        private Sensor MaxSizeOfMessagesAllowedOnProducerQueuesSensor;
        private Sensor TotalNumberOfRequestSentToKafkaSensor;
        private Sensor TotalNumberOfBytesTransmittedToKafkaSensor;

        //PER BROKER (add Broker NodeId as label)
        private LibrdKafkaSensor NumberOfRequestAwaitingTransmissionSensor;
        private LibrdKafkaSensor NumberOfMessagesAwaitingTransmissionSensor;
        private LibrdKafkaSensor NumberOfRequestInFlightSensor;
        private LibrdKafkaSensor NumberOfMessagesInFlightSensor;
        private LibrdKafkaSensor TotalNumberOfRequestSentSensor;
        private LibrdKafkaSensor TotalNumberOfBytesSentSensor;
        private LibrdKafkaSensor TotalNumberOfTransmissionErrorsSensor;
        private LibrdKafkaSensor TotalNumberOfRequestRetriesSensor;
        private LibrdKafkaSensor TotalNumberOfRequestTimeoutSensor;

        private LibrdKafkaSensor
            NumberOfConnectionAttempsSensor; // Including successful, failed and name resolution failures

        private LibrdKafkaSensor NumberOfDisconnectsSensor;
        private LibrdKafkaSensor InternalQueueProducerLatencyAverageMsSensor;
        private LibrdKafkaSensor InternalRequestQueueLatencyAverageMsSensor;
        private LibrdKafkaSensor BrokerLatencyAverageMsSensor;

        // Per Topic(add topic name as label)
        private LibrdKafkaSensor BatchSizeAverageBytesSensor;
        private LibrdKafkaSensor BatchMessageCountsAverageSensor;

        //  Per Partition(topic brokder id PartitionId as label)
        private LibrdKafkaSensor PartitionTotalNumberOfMessagesProducedSensor;
        private LibrdKafkaSensor PartitionTotalNumberOfBytesProducedSensor;
        private LibrdKafkaSensor PartitionNumberOfMessagesInFlightSensor;
        private LibrdKafkaSensor PartitionNextExpectedAckSequenceSensor;
        private LibrdKafkaSensor PartitionLastInternalMessageIdAckedSensor;

        public ProducerStatisticsHandler(
            string producerId,
            string streamAppId,
            string threadId = null,
            string taskId = null)
        {
            this.producerId = producerId;
            this.streamAppId = streamAppId;
            this.threadId = threadId ?? StreamMetricsRegistry.UNKNOWN_THREAD;
            this.taskId = taskId;
        }

        public void Register(StreamMetricsRegistry metricsRegistry)
        {
            TotalNumberOfMessagesProducedSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfMessagesProducedSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfMessageBytesProducedSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfMessageBytesProducedSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            NumberOfOpsWaitinInQueueSensor =
                LibrdKafkaProducerMetrics.NumberOfOpsWaitinInQueueSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            CurrentNumberOfMessagesInProducerQueuesSensor =
                LibrdKafkaProducerMetrics.CurrentNumberOfMessagesInProducerQueuesSensor(threadId, producerId,
                    streamAppId, metricsRegistry);
            CurrentSizeOfMessagesInProducerQueuesSensor =
                LibrdKafkaProducerMetrics.CurrentSizeOfMessagesInProducerQueuesSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            MaxMessagesAllowedOnProducerQueuesSensor =
                LibrdKafkaProducerMetrics.MaxMessagesAllowedOnProducerQueuesSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            MaxSizeOfMessagesAllowedOnProducerQueuesSensor =
                LibrdKafkaProducerMetrics.MaxSizeOfMessagesAllowedOnProducerQueuesSensor(threadId, producerId,
                    streamAppId, metricsRegistry);
            TotalNumberOfRequestSentToKafkaSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfRequestSentToKafkaSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfBytesTransmittedToKafkaSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfBytesTransmittedToKafkaSensor(threadId, producerId, streamAppId,
                    metricsRegistry);

            //PER BROKER (add Broker NodeId as label)
            NumberOfRequestAwaitingTransmissionSensor =
                LibrdKafkaProducerMetrics.NumberOfRequestAwaitingTransmissionSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            NumberOfMessagesAwaitingTransmissionSensor =
                LibrdKafkaProducerMetrics.NumberOfMessagesAwaitingTransmissionSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            NumberOfRequestInFlightSensor =
                LibrdKafkaProducerMetrics.NumberOfRequestInFlightSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            NumberOfMessagesInFlightSensor =
                LibrdKafkaProducerMetrics.NumberOfMessagesInFlightSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfRequestSentSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfRequestSentSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfBytesSentSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfBytesSentSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfTransmissionErrorsSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfTransmissionErrorsSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfRequestRetriesSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfRequestRetriesSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            TotalNumberOfRequestTimeoutSensor =
                LibrdKafkaProducerMetrics.TotalNumberOfRequestTimeoutSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            NumberOfConnectionAttempsSensor =
                LibrdKafkaProducerMetrics.NumberOfConnectionAttempsSensor(threadId, producerId, streamAppId,
                    metricsRegistry); // Including successful, failed and name resolution failures
            NumberOfDisconnectsSensor =
                LibrdKafkaProducerMetrics.NumberOfDisconnectsSensor(threadId, producerId, streamAppId, metricsRegistry);
            InternalQueueProducerLatencyAverageMsSensor =
                LibrdKafkaProducerMetrics.InternalQueueProducerLatencyAverageMsSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            InternalRequestQueueLatencyAverageMsSensor =
                LibrdKafkaProducerMetrics.InternalRequestQueueLatencyAverageMsSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            BrokerLatencyAverageMsSensor =
                LibrdKafkaProducerMetrics.BrokerLatencyAverageMsSensor(threadId, producerId, streamAppId,
                    metricsRegistry);

            // Per Topic(add topic name as label)
            BatchSizeAverageBytesSensor =
                LibrdKafkaProducerMetrics.BatchSizeAverageBytesSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            BatchMessageCountsAverageSensor =
                LibrdKafkaProducerMetrics.BatchMessageCountsAverageSensor(threadId, producerId, streamAppId,
                    metricsRegistry);

            //  Per Partition(topic brokder id PartitionId as label)
            PartitionTotalNumberOfMessagesProducedSensor =
                LibrdKafkaProducerMetrics.PartitionTotalNumberOfMessagesProducedSensor(threadId, producerId,
                    streamAppId, metricsRegistry);
            PartitionTotalNumberOfBytesProducedSensor =
                LibrdKafkaProducerMetrics.PartitionTotalNumberOfBytesProducedSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            PartitionNumberOfMessagesInFlightSensor =
                LibrdKafkaProducerMetrics.PartitionNumberOfMessagesInFlightSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            PartitionNextExpectedAckSequenceSensor =
                LibrdKafkaProducerMetrics.PartitionNextExpectedAckSequenceSensor(threadId, producerId, streamAppId,
                    metricsRegistry);
            PartitionLastInternalMessageIdAckedSensor =
                LibrdKafkaProducerMetrics.PartitionLastInternalMessageIdAckedSensor(threadId, producerId, streamAppId,
                    metricsRegistry);

            ApplyTaskIdTagIfNeed();
        }

        private void ApplyTaskIdTagIfNeed()
        {
            if (!string.IsNullOrEmpty(taskId))
            {
                TotalNumberOfMessagesProducedSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                TotalNumberOfMessageBytesProducedSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                NumberOfOpsWaitinInQueueSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                CurrentNumberOfMessagesInProducerQueuesSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                CurrentSizeOfMessagesInProducerQueuesSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                MaxMessagesAllowedOnProducerQueuesSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                MaxSizeOfMessagesAllowedOnProducerQueuesSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG,
                    taskId);
                TotalNumberOfRequestSentToKafkaSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                TotalNumberOfBytesTransmittedToKafkaSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                NumberOfRequestAwaitingTransmissionSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                NumberOfMessagesAwaitingTransmissionSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                NumberOfRequestInFlightSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                NumberOfMessagesInFlightSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                TotalNumberOfRequestSentSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                TotalNumberOfBytesSentSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                TotalNumberOfTransmissionErrorsSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                TotalNumberOfRequestRetriesSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                TotalNumberOfRequestTimeoutSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                NumberOfConnectionAttempsSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG,
                    taskId); // Including successful, failed and name resolution failures
                NumberOfDisconnectsSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                InternalQueueProducerLatencyAverageMsSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                InternalRequestQueueLatencyAverageMsSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                BrokerLatencyAverageMsSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                BatchSizeAverageBytesSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                BatchMessageCountsAverageSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                PartitionTotalNumberOfMessagesProducedSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                PartitionTotalNumberOfBytesProducedSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                PartitionNumberOfMessagesInFlightSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                PartitionNextExpectedAckSequenceSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
                PartitionLastInternalMessageIdAckedSensor.ChangeTagValue(StreamMetricsRegistry.TASK_ID_TAG, taskId);
            }
        }

        public void Publish(Statistics statistics)
        {
            TotalNumberOfMessagesProducedSensor.Record(statistics.TotalNumberOfMessagesProduced);
            TotalNumberOfMessageBytesProducedSensor.Record(statistics.TotalNumberOfMessageBytesProduced);
            NumberOfOpsWaitinInQueueSensor.Record(statistics.NumberOfOpsWaitinInQueue);
            CurrentNumberOfMessagesInProducerQueuesSensor.Record(statistics.CurrentNumberOfMessagesInProducerQueues);
            CurrentSizeOfMessagesInProducerQueuesSensor.Record(statistics.CurrentSizeOfMessagesInProducerQueues);
            MaxMessagesAllowedOnProducerQueuesSensor.Record(statistics.MaxMessagesAllowedOnProducerQueues);
            MaxSizeOfMessagesAllowedOnProducerQueuesSensor.Record(statistics.MaxSizeOfMessagesAllowedOnProducerQueues);
            TotalNumberOfRequestSentToKafkaSensor.Record(statistics.TotalNumberOfRequestSentToKafka);
            TotalNumberOfBytesTransmittedToKafkaSensor.Record(statistics.TotalNumberOfBytesTransmittedToKafka);

            PublishBrokerStats(statistics.Brokers);
            PublishTopicsStats(statistics.Topics);
        }
        
        private void PublishTopicsStats(Dictionary<string, TopicStatistic> statisticsTopics)
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

                foreach (var partition in topic.Value.Partitions)
                {
                    LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(PartitionTotalNumberOfMessagesProducedSensor
                        .Scoped(
                            (LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName),
                            (LibrdKafkaBaseMetrics.BROKER_ID_TAG, partition.Value.BrokerId.ToString()), 
                            (LibrdKafkaBaseMetrics.PARTITION_ID_TAG, partition.Value.PartitionId.ToString()))
                        , partition.Value.TotalNumberOfMessagesProduced, now);

                    LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(PartitionTotalNumberOfBytesProducedSensor
                        .Scoped(
                            (LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName),
                            (LibrdKafkaBaseMetrics.BROKER_ID_TAG, partition.Value.BrokerId.ToString()), 
                            (LibrdKafkaBaseMetrics.PARTITION_ID_TAG, partition.Value.PartitionId.ToString()))
                        , partition.Value.TotalNumberOfBytesProduced, now);

                    LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(PartitionNumberOfMessagesInFlightSensor
                        .Scoped(
                            (LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName),
                            (LibrdKafkaBaseMetrics.BROKER_ID_TAG, partition.Value.BrokerId.ToString()), 
                            (LibrdKafkaBaseMetrics.PARTITION_ID_TAG, partition.Value.PartitionId.ToString()))
                        , partition.Value.NumberOfMessagesInFlight, now);

                    LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(PartitionNextExpectedAckSequenceSensor
                        .Scoped(
                            (LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName),
                            (LibrdKafkaBaseMetrics.BROKER_ID_TAG, partition.Value.BrokerId.ToString()), 
                            (LibrdKafkaBaseMetrics.PARTITION_ID_TAG, partition.Value.PartitionId.ToString()))
                        , partition.Value.NextExpectedAckSequence, now);

                    LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(PartitionLastInternalMessageIdAckedSensor
                        .Scoped(
                            (LibrdKafkaBaseMetrics.TOPIC_TAG, topic.Value.TopicName),
                            (LibrdKafkaBaseMetrics.BROKER_ID_TAG, partition.Value.BrokerId.ToString()), 
                            (LibrdKafkaBaseMetrics.PARTITION_ID_TAG, partition.Value.PartitionId.ToString()))
                        , partition.Value.LastInternalMessageIdAcked, now);
                }

                BatchSizeAverageBytesSensor.RemoveOldScopeSensor(now);
                BatchMessageCountsAverageSensor.RemoveOldScopeSensor(now);
                PartitionTotalNumberOfMessagesProducedSensor.RemoveOldScopeSensor(now);
                PartitionTotalNumberOfBytesProducedSensor.RemoveOldScopeSensor(now);
                PartitionNumberOfMessagesInFlightSensor.RemoveOldScopeSensor(now);
                PartitionNextExpectedAckSequenceSensor.RemoveOldScopeSensor(now);
                PartitionLastInternalMessageIdAckedSensor.RemoveOldScopeSensor(now);
            }
        }

        private void PublishBrokerStats(Dictionary<string, BrokerStatistic> statisticsBrokers)
        {
            long now = DateTime.Now.GetMilliseconds();

            foreach (var broker in statisticsBrokers)
            {
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(NumberOfRequestAwaitingTransmissionSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.NumberOfRequestAwaitingTransmission, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(NumberOfMessagesAwaitingTransmissionSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.NumberOfMessagesAwaitingTransmission, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(NumberOfRequestInFlightSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.NumberOfRequestInFlight, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(NumberOfMessagesInFlightSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.NumberOfMessagesInFlight, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(TotalNumberOfRequestSentSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.TotalNumberOfRequestSent, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(TotalNumberOfBytesSentSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.TotalNumberOfBytesSent, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(TotalNumberOfTransmissionErrorsSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.TotalNumberOfTransmissionErrors, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(TotalNumberOfRequestRetriesSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.TotalNumberOfRequestRetries, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(TotalNumberOfRequestTimeoutSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.TotalNumberOfRequestTimeout, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(NumberOfConnectionAttempsSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.NumberOfConnectionAttemps, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(NumberOfDisconnectsSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.NumberOfDisconnects, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(InternalQueueProducerLatencyAverageMsSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.InternalQueueProducerLatency.Average, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(InternalRequestQueueLatencyAverageMsSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.InternalRequestQueueLatency.Average, now);
                LibrdKafkaSensor.ScopedLibrdKafkaSensor.Record(BrokerLatencyAverageMsSensor
                    .Scoped((LibrdKafkaBaseMetrics.BROKER_ID_TAG, broker.Value.NodeId.ToString()))
                    , broker.Value.BrokerLatency.Average, now);
            }

            NumberOfRequestAwaitingTransmissionSensor.RemoveOldScopeSensor(now);
            NumberOfMessagesAwaitingTransmissionSensor.RemoveOldScopeSensor(now);
            NumberOfRequestInFlightSensor.RemoveOldScopeSensor(now);
            NumberOfMessagesInFlightSensor.RemoveOldScopeSensor(now);
            TotalNumberOfRequestSentSensor.RemoveOldScopeSensor(now);
            TotalNumberOfBytesSentSensor.RemoveOldScopeSensor(now);
            TotalNumberOfTransmissionErrorsSensor.RemoveOldScopeSensor(now);
            TotalNumberOfRequestRetriesSensor.RemoveOldScopeSensor(now);
            TotalNumberOfRequestTimeoutSensor.RemoveOldScopeSensor(now);
            NumberOfConnectionAttempsSensor.RemoveOldScopeSensor(now);
            NumberOfDisconnectsSensor.RemoveOldScopeSensor(now);
            InternalQueueProducerLatencyAverageMsSensor.RemoveOldScopeSensor(now);
            InternalRequestQueueLatencyAverageMsSensor.RemoveOldScopeSensor(now);
            BrokerLatencyAverageMsSensor.RemoveOldScopeSensor(now);
        }
        
    }
}