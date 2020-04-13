using Confluent.Kafka;
using kafka_stream_core.Errors;
using kafka_stream_core.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using static kafka_stream_core.Mock.Kafka.MockConsumerInformation;

namespace kafka_stream_core.Mock.Kafka
{
    internal class MockConsumerInformation
    {
        internal class MockTopicPartitionOffset
        {
            public string Topic { get; set; }
            public int Partition { get; set; }
            public int OffsetComitted { get; set; }
            public int OffsetConsumed { get; set; }

            public override bool Equals(object obj)
            {
                return obj is MockTopicPartitionOffset &&
                    ((MockTopicPartitionOffset)obj).Topic.Equals(this.Topic) &&
                    ((MockTopicPartitionOffset)obj).Partition.Equals(this.Partition);
            }

            public override int GetHashCode()
            {
                return this.Topic.GetHashCode() & this.Partition.GetHashCode() ^ 33333;
            }
        }

        public string GroupId { get; set; }
        public string Name { get; set; }
        public List<TopicPartition> Partitions { get; set; }
        public List<string> Topics { get; set; }
        public MockConsumer Customer { get; set; }
        public IConsumerRebalanceListener RebalanceListener { get; set; }
        public bool Assigned { get; set; } = false;
        public List<MockTopicPartitionOffset> TopicPartitionsOffset { get; set; }

        public override int GetHashCode()
        {
            return this.Name.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            return obj is MockConsumerInformation && ((MockConsumerInformation)obj).Name.Equals(this.Name);
        }
    }

    internal class MockCluster
    {
        #region Singleton

        private static object _lock = new object();
        private static MockCluster cluster = null;

        private MockCluster() { }

        public static MockCluster Instance
        {
            get
            {
                lock (_lock)
                {
                    if (cluster == null)
                        cluster = new MockCluster();
                }
                return cluster;
            }
        }

        #endregion

        private readonly IList<MockTopic> topics = new List<MockTopic>();
        private readonly IDictionary<string, MockConsumerInformation> consumers = new Dictionary<string, MockConsumerInformation>();
        private readonly IDictionary<string, int> consumersOffsets = new Dictionary<string, int>();
        private readonly IDictionary<string, List<string>> consumerGroups = new Dictionary<string, List<string>>();

        #region Topic Gesture

        private bool CreateTopic(string topic, int partitions = 4)
        {
            if (!topics.Any(t => t.Name.Equals(topic, StringComparison.InvariantCultureIgnoreCase)))
            {
                var t = new MockTopic(topic, partitions);
                topics.Add(t);
                return true;
            }
            return false;
        }

        internal void CloseConsumer(string name)
        {
            this.Unsubscribe(this.consumers[name].Customer);
            consumers.Remove(name);
        }

        internal void SubscribeTopic(MockConsumer consumer, IEnumerable<string> topics)
        {
            foreach (var t in topics)
                this.CreateTopic(t);

            if (consumers.ContainsKey(consumer.Name))
            {
                var cons = new MockConsumerInformation
                {
                    GroupId = consumer.MemberId,
                    Name = consumer.Name,
                    Customer = consumer,
                    Topics = new List<string>(topics),
                    RebalanceListener = consumer.Listener,
                    Partitions = new List<TopicPartition>(),
                    TopicPartitionsOffset = new List<MockConsumerInformation.MockTopicPartitionOffset>()
                };
                consumers.Add(consumer.Name, cons);

                if (consumerGroups.ContainsKey(consumer.MemberId))
                    consumerGroups[consumer.MemberId].Add(consumer.Name);
                else
                    consumerGroups.Add(consumer.MemberId, new List<string> { consumer.Name });
            }
            else
                throw new StreamsException($"Client {consumer.Name} already subscribe topic. Please call unsucribe before");
        }

        internal void Unsubscribe(MockConsumer mockConsumer)
        {
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                this.Unassign(mockConsumer);
                c.Topics.Clear();
            }
        }

        #endregion

        #region Partitions Gesture

        internal void Assign(MockConsumer mockConsumer, IEnumerable<TopicPartition> topicPartitions)
        {
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                if (c.Partitions.Count == 0)
                {
                    List<MockConsumerInformation> customerToRebalance = new List<MockConsumerInformation>();

                    foreach (var p in topicPartitions)
                    {
                        var info = consumers.Select(kp => kp.Value)
                            .Where(i => i.GroupId.Equals(mockConsumer.MemberId))
                            .FirstOrDefault(i => i.Partitions.Contains(p));
                        if (info != null && !customerToRebalance.Contains(info))
                            customerToRebalance.Add(info);
                    }

                    foreach (var cus in customerToRebalance)
                    {
                        var parts = cus.Partitions.Join(topicPartitions, (t) => t, (t) => t, (t1, t2) => t1);
                        var pList = cus.TopicPartitionsOffset.Select(f => new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetComitted)).ToList();
                        cus.RebalanceListener.PartitionsRevoked(cus.Customer, pList);
                        foreach (var j in parts)
                            cus.Partitions.Remove(j);

                        cus.RebalanceListener.PartitionsAssigned(cus.Customer, cus.Partitions);
                    }

                    c.Partitions = new List<TopicPartition>(topicPartitions);
                    foreach (var k in topicPartitions)
                    {
                        if (!c.TopicPartitionsOffset.Any(m => m.Topic.Equals(k.Topic) && m.Partition.Equals(k.Partition)))
                            c.TopicPartitionsOffset.Add(new MockTopicPartitionOffset
                            {
                                OffsetComitted = 0,
                                OffsetConsumed = 0,
                                Partition = k.Partition,
                                Topic = k.Topic
                            });
                    }
                    c.RebalanceListener.PartitionsAssigned(c.Customer, c.Partitions);
                }
                else
                    throw new StreamsException($"Consumer {mockConsumer.Name} was already assigned partitions. Please call unassigne before !");
            }
            else
                throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
        }

        internal void Unassign(MockConsumer mockConsumer)
        {
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                var pList = c.TopicPartitionsOffset.Select(f => new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetComitted)).ToList();
                c.RebalanceListener.PartitionsRevoked(c.Customer, pList);

                // Rebalance on other consumer in the same group
                var otherConsumers = consumerGroups[mockConsumer.MemberId].Select(i => consumers[i]).Where(i => !i.Name.Equals(mockConsumer.Name)).ToList();
                int partEach = (int)(c.Partitions.Count / otherConsumers.Count);
                int modulo = c.Partitions.Count % otherConsumers.Count;

                int j = 0;
                for (int i = 0; i < otherConsumers.Count; ++i)
                {
                    List<TopicPartition> parts = null;
                    if (i == otherConsumers.Count - 1)
                    {
                        parts = c.Partitions.GetRange(j, j + partEach + modulo);
                    }
                    else
                    {
                        parts = c.Partitions.GetRange(j, j + partEach);
                    }

                    otherConsumers[i].Partitions.AddRange(parts);
                    foreach (var k in parts)
                        if (!otherConsumers[i].TopicPartitionsOffset.Any(m => m.Topic.Equals(k.Topic) && m.Partition.Equals(k.Partition)))
                            otherConsumers[i].TopicPartitionsOffset.Add(new MockTopicPartitionOffset
                            {
                                OffsetComitted = 0,
                                OffsetConsumed = 0,
                                Partition = k.Partition,
                                Topic = k.Topic
                            });

                    otherConsumers[i].RebalanceListener.PartitionsAssigned(otherConsumers[i].Customer, otherConsumers[i].Partitions);
                    j += partEach;
                }

                c.Partitions.Clear();
            }
            else
                throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");

        }

        #endregion

        #region Consumer (Read + Commit) Gesture

        internal List<TopicPartitionOffset> Commit(MockConsumer mockConsumer)
        {
            throw new NotImplementedException();
        }

        internal void Commit(MockConsumer mockConsumer, IEnumerable<TopicPartitionOffset> offsets)
        {
            throw new NotImplementedException();
        }

        internal ConsumeResult<byte[], byte[]> Consume(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        internal ConsumeResult<byte[], byte[]> Consume(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
