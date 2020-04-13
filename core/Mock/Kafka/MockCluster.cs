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
            public long OffsetComitted { get; set; }
            public long OffsetConsumed { get; set; }

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
        public MockConsumer Consumer { get; set; }
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

        private readonly IDictionary<string ,MockTopic> topics = new Dictionary<string, MockTopic>();
        private readonly IDictionary<string, MockConsumerInformation> consumers = new Dictionary<string, MockConsumerInformation>();
        private readonly IDictionary<string, List<string>> consumerGroups = new Dictionary<string, List<string>>();

        #region Topic Gesture

        private bool CreateTopic(string topic, int partitions = 4)
        {
            if (!topics.Values.Any(t => t.Name.Equals(topic, StringComparison.InvariantCultureIgnoreCase)))
            {
                var t = new MockTopic(topic, partitions);
                topics.Add(topic, t);
                return true;
            }
            return false;
        }

        internal void CloseConsumer(string name)
        {
            this.Unsubscribe(this.consumers[name].Consumer);
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
                    Consumer = consumer,
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

        private void CheckAssignedPartitions()
        {
            foreach (var group in consumerGroups.Keys)
            {
                var consumers = consumerGroups[group];
                foreach(var c in consumers)
                {
                    var consumer = this.consumers[c];
                    if(!consumer.Assigned)
                    {
                        // TODO : 
                    }
                }
            }
        }

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
                        cus.RebalanceListener.PartitionsRevoked(cus.Consumer, pList);
                        foreach (var j in parts)
                            cus.Partitions.Remove(j);

                        cus.RebalanceListener.PartitionsAssigned(cus.Consumer, cus.Partitions);
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
                    c.RebalanceListener.PartitionsAssigned(c.Consumer, c.Partitions);
                    c.Assigned = true;
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
                c.RebalanceListener.PartitionsRevoked(c.Consumer, pList);

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

                    otherConsumers[i].RebalanceListener.PartitionsAssigned(otherConsumers[i].Consumer, otherConsumers[i].Partitions);
                    j += partEach;
                }

                c.Partitions.Clear();
                c.Assigned = false;
            }
            else
                throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");

        }

        #endregion

        #region Consumer (Read + Commit) Gesture

        internal List<TopicPartitionOffset> Commit(MockConsumer mockConsumer)
        {
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                foreach (var p in c.TopicPartitionsOffset)
                    p.OffsetComitted = p.OffsetConsumed;

                return c.TopicPartitionsOffset.Select(t => new TopicPartitionOffset(new TopicPartition(t.Topic, t.Partition), t.OffsetComitted)).ToList();
            }
            else
                throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
        }

        internal void Commit(MockConsumer mockConsumer, IEnumerable<TopicPartitionOffset> offsets)
        {
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                foreach(var o in offsets)
                {
                    var p = c.TopicPartitionsOffset.FirstOrDefault(t => t.Topic.Equals(o.Topic) && t.Partition.Equals(o.Partition));
                    if(p != null)
                    {
                        p.OffsetConsumed = o.Offset.Value;
                        p.OffsetComitted = o.Offset.Value;
                    }
                }
            }
            else
                throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
        }

        internal ConsumeResult<byte[], byte[]> Consume(MockConsumer mockConsumer, TimeSpan timeout)
        {
            DateTime dt = DateTime.Now;
            ConsumeResult<byte[], byte[]> result = null;
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                if (c.Partitions.Count == 0)
                    CheckAssignedPartitions();

                foreach (var p in c.Partitions)
                {
                    if ((dt += timeout) < DateTime.Now)
                        break;
                    var topic = topics[p.Topic];
                    var offset = c.TopicPartitionsOffset.FirstOrDefault(t => t.Topic.Equals(p.Topic) && t.Partition.Equals(p.Partition));
                    var record = topic.GetMessage(p.Partition, offset.OffsetConsumed);
                    if (record != null)
                    {
                        result = new ConsumeResult<byte[], byte[]>
                        {
                            Offset = offset.OffsetConsumed,
                            Topic = p.Topic,
                            Partition = p.Partition,
                            Message = new Message<byte[], byte[]> { Key = record.Key, Value = record.Value }
                        };
                        ++offset.OffsetConsumed;
                        break;
                    }
                }

                return result;
            }
            else
                throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
        }

        internal ConsumeResult<byte[], byte[]> Consume(MockConsumer mockConsumer, CancellationToken cancellationToken)
            => Consume(mockConsumer, TimeSpan.FromSeconds(10));

        #endregion

        #region Producer Gesture

        #endregion
    }
}
