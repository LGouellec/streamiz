using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using static Streamiz.Kafka.Net.Mock.Kafka.MockConsumerInformation;

namespace Streamiz.Kafka.Net.Mock.Kafka
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
        internal static int DEFAULT_NUMBER_PARTITIONS = 4;

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

        private readonly IDictionary<string, MockTopic> topics = new Dictionary<string, MockTopic>();
        private readonly IDictionary<string, MockConsumerInformation> consumers = new Dictionary<string, MockConsumerInformation>();
        private readonly IDictionary<string, List<string>> consumerGroups = new Dictionary<string, List<string>>();

        #region Topic Gesture

        private bool CreateTopic(string topic) => CreateTopic(topic, DEFAULT_NUMBER_PARTITIONS);

        private bool CreateTopic(string topic, int partitions)
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

            if (!consumers.ContainsKey(consumer.Name))
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

        internal List<TopicPartitionOffset> Comitted(MockConsumer mockConsumer)
        {
            var c = consumers[mockConsumer.Name];
            List<TopicPartitionOffset> list = new List<TopicPartitionOffset>();
            foreach(var p in c.Partitions)
            {
                var offset = c.TopicPartitionsOffset.FirstOrDefault(t => t.Topic.Equals(p.Topic) && t.Partition.Equals(p.Partition));
                if (offset != null)
                    list.Add(new TopicPartitionOffset(new TopicPartition(p.Topic, p.Partition), new Offset(offset.OffsetComitted)));
            }
            return list;
        }

        internal WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
        {
            var topic = topics[topicPartition.Topic];
            var p = topic.GetPartition(topicPartition.Partition);
            return new WatermarkOffsets(new Offset(p.LowOffset), new Offset(p.HighOffset));
        }

        private IEnumerable<TopicPartition> Generate(string topic, int start, int number)
        {
            for (int i = start; i < start + number; ++i)
                yield return new TopicPartition(topic, i);
        }

        private void NeedRebalance()
        {
            foreach (var group in consumerGroups.Keys)
            {
                var map = new Dictionary<string, List<MockConsumerInformation>>();
                var consumers = consumerGroups[group];
                foreach (var c in consumers)
                {
                    var consumer = this.consumers[c];
                    consumer.Topics.ForEach((t) =>
                    {
                        if (map.ContainsKey(t))
                            map[t].Add(consumer);
                        else
                            map.Add(t, new List<MockConsumerInformation> { consumer });
                    });
                }

                foreach (var kv in map)
                {
                    if (kv.Value.Any(c => !c.Assigned))
                    {
                        var topicPartitionNumber = this.topics[kv.Key].PartitionNumber;
                        int numbPartEach = topicPartitionNumber / kv.Value.Count;
                        int modulo = topicPartitionNumber % kv.Value.Count;

                        int j = 0;
                        for (int i = 0; i < kv.Value.Count; ++i)
                        {
                            List<TopicPartition> parts = null;
                            if (i == kv.Value.Count - 1)
                            {
                                parts = new List<TopicPartition>(Generate(kv.Key, j, numbPartEach + modulo));
                            }
                            else
                            {
                                parts = new List<TopicPartition>(Generate(kv.Key, j, numbPartEach));
                            }

                            if (kv.Value[i].Partitions.Count > 0)
                            {
                                kv.Value[i].Partitions.Clear();
                                var pList = kv.Value[i].TopicPartitionsOffset.Select(f => new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetComitted)).ToList();
                                kv.Value[i].RebalanceListener?.PartitionsRevoked(kv.Value[i].Consumer, pList);
                            }

                            kv.Value[i].Partitions.AddRange(parts);
                            foreach (var k in parts)
                                if (!kv.Value[i].TopicPartitionsOffset.Any(m => m.Topic.Equals(k.Topic) && m.Partition.Equals(k.Partition)))
                                    kv.Value[i].TopicPartitionsOffset.Add(new MockTopicPartitionOffset
                                    {
                                        OffsetComitted = 0,
                                        OffsetConsumed = 0,
                                        Partition = k.Partition,
                                        Topic = k.Topic
                                    });

                            kv.Value[i].RebalanceListener?.PartitionsAssigned(kv.Value[i].Consumer, kv.Value[i].Partitions);
                            kv.Value[i].Assigned = true;
                            j += numbPartEach;
                        }
                    }
                }
            }
        }

        internal void Assign(MockConsumer mockConsumer, IEnumerable<TopicPartition> topicPartitions)
        {
            foreach (var t in topicPartitions)
                this.CreateTopic(t.Topic);

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
                        cus.RebalanceListener?.PartitionsRevoked(cus.Consumer, pList);
                        foreach (var j in parts)
                            cus.Partitions.Remove(j);

                        cus.RebalanceListener?.PartitionsAssigned(cus.Consumer, cus.Partitions);
                        cus.Assigned = true;
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
                    c.RebalanceListener?.PartitionsAssigned(c.Consumer, c.Partitions);
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
                c.RebalanceListener?.PartitionsRevoked(c.Consumer, pList);

                // Rebalance on other consumer in the same group
                var otherConsumers = consumerGroups[mockConsumer.MemberId].Where(i => consumers.ContainsKey(i)).Select(i => consumers[i]).Where(i => !i.Name.Equals(mockConsumer.Name)).ToList();
                if (otherConsumers.Count > 0)
                {
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

                        otherConsumers[i].RebalanceListener?.PartitionsAssigned(otherConsumers[i].Consumer, otherConsumers[i].Partitions);
                        otherConsumers[i].Assigned = true;
                        j += partEach;
                    }
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
                foreach (var o in offsets)
                {
                    var p = c.TopicPartitionsOffset.FirstOrDefault(t => t.Topic.Equals(o.Topic) && t.Partition.Equals(o.Partition));
                    if (p != null)
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
            foreach (var t in mockConsumer.Subscription)
                this.CreateTopic(t);

            DateTime dt = DateTime.Now;
            ConsumeResult<byte[], byte[]> result = null;
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                if (!c.Assigned)
                    NeedRebalance();

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

        internal DeliveryReport<byte[], byte[]> Produce(string topic, Message<byte[], byte[]> message)
        {
            DeliveryReport<byte[], byte[]> r = new DeliveryReport<byte[], byte[]>();

            CreateTopic(topic);
            Random rd = new Random();
            var i = rd.Next(0, this.topics[topic].PartitionNumber);
            this.topics[topic].AddMessage(message.Key, message.Value, i);

            r.Message = message;
            r.Partition = i;
            r.Topic = topic;
            r.Timestamp = new Timestamp(DateTime.Now);
            r.Error = new Error(ErrorCode.NoError);
            r.Status = PersistenceStatus.Persisted;
            return r;
        }

        internal DeliveryReport<byte[], byte[]> Produce(TopicPartition topicPartition, Message<byte[], byte[]> message)
        {
            DeliveryReport<byte[], byte[]> r = new DeliveryReport<byte[], byte[]>();
            r.Status = PersistenceStatus.NotPersisted;
            CreateTopic(topicPartition.Topic);
            if (this.topics[topicPartition.Topic].PartitionNumber > topicPartition.Partition)
            {
                this.topics[topicPartition.Topic].AddMessage(message.Key, message.Value, topicPartition.Partition);
                r.Status = PersistenceStatus.Persisted;
            }
            else
            {
                this.topics[topicPartition.Topic].CreateNewPartitions(topicPartition.Partition);
                this.topics[topicPartition.Topic].AddMessage(message.Key, message.Value, topicPartition.Partition);
                r.Status = PersistenceStatus.Persisted;
            }
            r.Message = message;
            r.Partition = topicPartition.Partition;
            r.Topic = topicPartition.Topic;
            r.Timestamp = new Timestamp(DateTime.Now);
            r.Error = new Error(ErrorCode.NoError);
            r.Status = PersistenceStatus.Persisted;
            // TODO r.Offset
            return r;
        }

        #endregion
    }
}
