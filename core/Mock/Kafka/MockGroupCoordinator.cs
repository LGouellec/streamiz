using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockGroupCoordinator
    {
        private readonly object _lock = new object();
        private readonly ILog log = Logger.GetLogger(typeof(MockGroupCoordinator));

        private readonly MockCluster cluster = null;
        internal class MockGroupCoordinatorState
        {
            public MockConsumerInformation Consumer { get; set; }
            public bool RebalanceRevokedPartitions { get; set; } = false;
            public bool RebalanceAssignedPartititons { get; set; } = false;
        }
        public ConcurrentDictionary<string, Dictionary<MockConsumerInformation, MockGroupCoordinatorState>> RebalanceState
        {
            get;
            set;
        } = new();

        public MockGroupCoordinator(MockCluster cluster)
        {
            this.cluster = cluster;
        }
        
        public string NewRebalanceTransaction(List<MockConsumerInformation> consumers)
        {
            lock (_lock)
            {
                string rebalanceId = Guid.NewGuid().ToString();

                Dictionary<MockConsumerInformation, MockGroupCoordinatorState> infos = new();
                foreach (var c in consumers)
                    infos.Add(c, new MockGroupCoordinatorState()
                    {
                        Consumer = c,
                        RebalanceAssignedPartititons = false,
                        RebalanceRevokedPartitions = false
                    });

                RebalanceState.TryAdd(rebalanceId, infos);
                return  rebalanceId;
            }
        }

        public bool TryToCompleteRebalancing(string rebalanceIdTransaction, string customerName)
        {
            if (!string.IsNullOrEmpty(rebalanceIdTransaction) && RebalanceState.ContainsKey(rebalanceIdTransaction))
            {
                var consumers = RebalanceState[rebalanceIdTransaction];
                var consumer = consumers.FirstOrDefault(c => c.Key.Name.Equals(customerName));
                if (consumer.Key.NeedRebalanceTriggered)
                {
                    if (consumer.Key.PartitionsToRevoked != null && consumer.Key.PartitionsToRevoked.Any())
                    {
                        foreach (var tp in consumer.Key.PartitionsToRevoked)
                        {
                            consumer.Key.Partitions.Remove(tp.TopicPartition);
                            consumer.Key.TopicPartitionsOffset.Remove(new MockConsumerInformation.MockTopicPartitionOffset
                            {
                                Partition = tp.Partition.Value,
                                Topic = tp.Topic
                            });
                        }

                        consumer.Key.RebalanceListener?.PartitionsRevoked(consumer.Key.Consumer, consumer.Key.PartitionsToRevoked);
                        consumer.Key.PartitionsToRevoked?.Clear();
                    }
                    
                    consumer.Value.RebalanceRevokedPartitions = true;

                    bool allRevokedFinished = consumers.All(c => c.Value.RebalanceRevokedPartitions);
                    if (allRevokedFinished)
                    {
                        if (consumer.Key.PartitionsToAssigned != null && consumer.Key.PartitionsToAssigned.Any())
                        {
                            foreach (var tp in consumer.Key.PartitionsToAssigned)
                            {
                                var groupOffset = cluster.GetGroupOffset(consumer.Key.GroupId, tp);
                                consumer.Key.Partitions.Add(tp);
                                consumer.Key.TopicPartitionsOffset.Add(new MockConsumerInformation.MockTopicPartitionOffset
                                {
                                    Partition = tp.Partition.Value,
                                    Topic = tp.Topic,
                                    IsPaused = false,
                                    OffsetComitted = groupOffset.Offset,
                                    OffsetConsumed = groupOffset.Offset
                                });
                            }

                            consumer.Key.RebalanceListener?.PartitionsAssigned(consumer.Key.Consumer, consumer.Key.PartitionsToAssigned);
                        }
                        
                        consumer.Key.Assigned = true;
                        consumer.Key.NeedRebalanceTriggered = false;
                        consumer.Value.RebalanceAssignedPartititons = true;
                    }
                }

                bool allRebalanceFinished = consumers.All(c => c.Value.RebalanceRevokedPartitions && c.Value.RebalanceAssignedPartititons);
                if (allRebalanceFinished)
                {
                    foreach (var kvc in consumers.Keys)
                        kvc.RebalanceId = null;
                    
                    RebalanceState.TryRemove(rebalanceIdTransaction, out Dictionary<MockConsumerInformation, MockGroupCoordinatorState> dic);
                    log.Info($"Rebalance {rebalanceIdTransaction} finished !");
                    return true;
                }

                return consumer.Value.RebalanceAssignedPartititons;
            }

            return true;
        }
    }
}