﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class InternalTopicManagerUtils
    {
        internal static ILogger log = Logger.GetLogger(typeof(InternalTopicManagerUtils));
        private static readonly TimeSpan timeout = TimeSpan.FromSeconds(10);
        private static readonly ConfigResource brokerConfigResource;

        static InternalTopicManagerUtils()
        {
            brokerConfigResource = new ConfigResource
            {
                Type = ResourceType.Broker
            };
        }

        internal static InternalTopicManagerUtils New() => new();

        internal async Task CreateInternalTopicsAsync(
            ITopicManager topicManager,
            InternalTopologyBuilder builder)
        {
            //var clusterMetadata = topicManager.AdminClient.GetMetadata(timeout);
            var internalTopicsGroups = builder.MakeInternalTopicGroups();
            
            foreach (var entry in internalTopicsGroups)
            {
                ComputeRepartitionTopicConfig(entry.Value, internalTopicsGroups, topicManager);
                ComputeChangelogTopicConfig(entry.Value, topicManager);

                var internalTopics = entry.Value.ChangelogTopics.Union(entry.Value.RepartitionTopics).ToDictionary();

                await topicManager.ApplyAsync(entry.Key, internalTopics);
            }
        }

        private static void ComputeChangelogTopicConfig(
            InternalTopologyBuilder.TopologyTopicsInfo topicsInfo,
            ITopicManager topicManager)
        {
            var metadata =
                topicManager.AdminClient.GetMetadata(topicsInfo.SourceTopics.First(), TimeSpan.FromSeconds(10));
            var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic.Equals(topicsInfo.SourceTopics.First()));
            if (topicMetadata != null)
            {
                topicsInfo
                    .ChangelogTopics
                    .Values
                    .ForEach(c => c.NumberPartitions = topicMetadata.Partitions.Count);
            }
            else
            {
                topicsInfo
                    .ChangelogTopics
                    .Values
                    .ForEach(c => c.NumberPartitions = -1);
            }
        }

        private static void ComputeRepartitionTopicConfig(
            InternalTopologyBuilder.TopologyTopicsInfo topicsInfo,
            IDictionary<int, InternalTopologyBuilder.TopologyTopicsInfo> topologyTopicInfos,
            ITopicManager topicManager)
        {
            if (topicsInfo.RepartitionTopics.Any())
            {
                CheckIfExternalSourceTopicsExist(topicsInfo, topicManager);
                SetRepartitionSourceTopicPartitionCount(topicsInfo.RepartitionTopics, topologyTopicInfos, topicManager);
            }
        }

        private static void CheckIfExternalSourceTopicsExist(
            InternalTopologyBuilder.TopologyTopicsInfo topicsInfo,
            ITopicManager topicManager)
        {
            List<string> sourcesTopics = new List<string>();
            foreach (var s in topicsInfo.SourceTopics)
            {
                var metadata = topicManager.AdminClient.GetMetadata(s, TimeSpan.FromSeconds(10));
                if (metadata.PartitionCountForTopic(s).HasValue)
                    sourcesTopics.Add(s);
            }
            
            if (!sourcesTopics.Any())
            {
                log.LogError($"Topology use one (or multiple) repartition topic(s)." +
                             $" The following source topics ({string.Join(",", sourcesTopics)}) are missing/unknown." +
                             $" Please make sure all sources topics have been-created before starting the streams application.");
                throw new StreamsException($"Missing source topics : {string.Join(",", sourcesTopics)}.");
            }
        }

        private static void SetRepartitionSourceTopicPartitionCount(
            IDictionary<string, InternalTopicConfig> repartitionTopics,
            IDictionary<int, InternalTopologyBuilder.TopologyTopicsInfo> topologyTopicInfos,
            ITopicManager topicManager)
        {
            #region Compute topic partition count

            int? ComputePartitionCount(string repartitionTopic)
            {
                int? partitionCount = null;
                foreach (var topologyTopicsInfo in topologyTopicInfos.Values)
                {
                    if (topologyTopicsInfo.SinkTopics.Contains(repartitionTopic))
                    {
                        foreach (var upstreamSourceTopic in topologyTopicsInfo.SourceTopics)
                        {
                            int? numPartitionCandidate = null;

                            if (repartitionTopics.ContainsKey(upstreamSourceTopic))
                            {
                                numPartitionCandidate = repartitionTopics[upstreamSourceTopic].NumberPartitions;
                            }
                            else
                            {
                                var clusterMetadata =
                                    topicManager.AdminClient.GetMetadata(upstreamSourceTopic, TimeSpan.FromSeconds(10));
                                var count = clusterMetadata.PartitionCountForTopic(upstreamSourceTopic);
                                if (count == null)
                                    count = ComputePartitionCount(upstreamSourceTopic);
                                // always null ?
                                if (count == null)
                                    throw new StreamsException(
                                        $"No partition count found for source topic {upstreamSourceTopic}, but I should have been.");
                                numPartitionCandidate = count;
                            }

                            if (numPartitionCandidate != null && (partitionCount == null || numPartitionCandidate > partitionCount))
                                partitionCount = numPartitionCandidate;
                        }
                    }
                }
                return partitionCount;
            }

            #endregion

            foreach (var repartitionTopic in repartitionTopics)
            {
                if (repartitionTopic.Value.NumberPartitions == 0)
                {
                    var numberPartition = ComputePartitionCount(repartitionTopic.Key);
                    if (!numberPartition.HasValue)
                    {
                        log.LogWarning(
                            $"Unable to determine number of partitions for {repartitionTopic}.");
                        throw new StreamsException($"Unable to determine number of partitions for {repartitionTopic}.");
                    }

                    repartitionTopic.Value.NumberPartitions = numberPartition.Value;
                }
            }
        }

        internal static int DefaultPartitionNumber(List<DescribeConfigsResult> configsResults)
        {
            string numPartitionsCst = "num.partitions";
            if (configsResults.First().Entries.ContainsKey(numPartitionsCst))
                return int.Parse(configsResults.First().Entries[numPartitionsCst].Value);
            throw new StreamsException("Default number partitions unavailable !");
        }

        // Use for testing (TaskSynchronousTopologyDriver & ClusterInMemoryTopologyDriver) to create source topics before repartition & changelog topcis
        internal InternalTopicManagerUtils CreateSourceTopics(InternalTopologyBuilder builder, IKafkaSupplier supplier)
        {
            var adminConfig = new AdminClientConfig();
            adminConfig.ClientId = "internal-admin-create-soure-topic";

            var sourceTopics = builder.BuildTopology().GetSourceTopics().ToList();
            var globalTopo = builder.BuildGlobalStateTopology();
            if (globalTopo != null)
                sourceTopics.AddRange(globalTopo.StoresToTopics.Values);

            supplier
                .GetAdmin(adminConfig)
                .CreateTopicsAsync(sourceTopics.Select(s => new TopicSpecification()
                {
                    Name = s,
                    NumPartitions = -1
                })).GetAwaiter().GetResult();

            return this;
        }
    }
}
