            using Confluent.Kafka;
            using Streamiz.Kafka.Net.Crosscutting;
            using Streamiz.Kafka.Net.Errors;
            using Streamiz.Kafka.Net.Kafka;
            using System;
            using System.Collections.Generic;
            using System.Linq;
            using System.Security.Cryptography;
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
                        public bool IsPaused { get; set; } = false;

                        public TopicPartition TopicPartition => new TopicPartition(Topic, Partition);

                        public override bool Equals(object obj)
                        {
                            return obj is MockTopicPartitionOffset &&
                                ((MockTopicPartitionOffset)obj).Topic.Equals(Topic) &&
                                ((MockTopicPartitionOffset)obj).Partition.Equals(Partition);
                        }

                        public override int GetHashCode()
                        {
                            return Topic.GetHashCode() & Partition.GetHashCode() ^ 33333;
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
                        return Name.GetHashCode();
                    }

                    public override bool Equals(object obj)
                    {
                        return obj is MockConsumerInformation && ((MockConsumerInformation)obj).Name.Equals(Name);
                    }
                }

                internal class MockCluster
                {
                    private static readonly object _lock = new object();
                    internal readonly int DEFAULT_NUMBER_PARTITIONS;

                    #region Ctor

                    public MockCluster(int defaultNumberPartitions = 1)
                    {
                        DEFAULT_NUMBER_PARTITIONS = defaultNumberPartitions;
                    }

                    public void Destroy()
                    {
                        topics.Clear();
                        consumers.Clear();
                        consumerGroups.Clear();
                    }

                    #endregion

                    private readonly IDictionary<string, MockTopic> topics = new Dictionary<string, MockTopic>();
                    private readonly IDictionary<string, MockConsumerInformation> consumers = new Dictionary<string, MockConsumerInformation>();
                    private readonly IDictionary<string, List<string>> consumerGroups = new Dictionary<string, List<string>>();

                    #region Topic Gesture

                    internal void CreateTopic(string topic) => CreateTopic(topic, DEFAULT_NUMBER_PARTITIONS);

                    internal bool CreateTopic(string topic, int partitions)
                    {
                        lock (_lock)
                        {
                            if (!topics.Values.Any(t => t.Name.Equals(topic, StringComparison.InvariantCultureIgnoreCase)))
                            {
                                var t = new MockTopic(topic, partitions);
                                topics.Add(topic, t);
                                return true;
                            }
                        }
                        return false;
                    }

                    internal void Pause(MockConsumer mockConsumer, IEnumerable<TopicPartition> enumerable)
                    {
                        lock (_lock)
                        {
                            if (consumers.ContainsKey(mockConsumer.Name))
                            {
                                foreach (var tpo in consumers[mockConsumer.Name].TopicPartitionsOffset)
                                {
                                    if (enumerable.Contains(tpo.TopicPartition))
                                    {
                                        tpo.IsPaused = true;
                                    }
                                }
                            }
                        }
                    }

                    internal void CloseConsumer(string name)
                    {
                        if (consumers.ContainsKey(name))
                        {
                            Unsubscribe(consumers[name].Consumer);
                            consumers.Remove(name);
                        }
                    }

                    internal void Resume(MockConsumer mockConsumer, IEnumerable<TopicPartition> enumerable)
                    {
                        lock (_lock)
                        {
                            if (consumers.ContainsKey(mockConsumer.Name))
                            {
                                foreach (var tpo in consumers[mockConsumer.Name].TopicPartitionsOffset)
                                {
                                    if (enumerable.Contains(tpo.TopicPartition))
                                    {
                                        tpo.IsPaused = false;
                                    }
                                }
                            }
                        }
                    }

                    internal void Seek(MockConsumer mockConsumer, TopicPartitionOffset tpo)
                    {
                        lock (_lock)
                        {
                            if (consumers.ContainsKey(mockConsumer.Name))
                            {
                                foreach (var tpos in consumers[mockConsumer.Name].TopicPartitionsOffset)
                                {
                                    if (tpos.TopicPartition.Equals(tpo.TopicPartition))
                                    {
                                        if (tpo.Offset == Offset.Beginning)
                                            tpos.OffsetConsumed = 0;
                                        else if (tpo.Offset == Offset.End)
                                        {
                                            var topic = topics[tpo.Topic];
                                            var part = topic.GetPartition(tpo.Partition);
                                            tpos.OffsetConsumed = part.HighOffset;
                                        }
                                        else
                                            tpos.OffsetConsumed = tpo.Offset;
                                    }
                                }
                            }
                        }
                    }

                    internal void SubscribeTopic(MockConsumer consumer, IEnumerable<string> topics)
                    {
                        foreach (var t in topics)
                        {
                            CreateTopic(t);
                        }

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
                                Assigned = false,
                                TopicPartitionsOffset = new List<MockConsumerInformation.MockTopicPartitionOffset>()
                            };
                            consumers.Add(consumer.Name, cons);

                            if (consumerGroups.ContainsKey(consumer.MemberId))
                            {
                                consumerGroups[consumer.MemberId].Add(consumer.Name);
                            }
                            else
                            {
                                consumerGroups.Add(consumer.MemberId, new List<string> { consumer.Name });
                            }
                        }
                        else
                        {
                            throw new StreamsException($"Client {consumer.Name} already subscribe topic. Please call unsucribe before");
                        }
                    }

                    internal void Unsubscribe(MockConsumer mockConsumer)
                    {
                        if (consumers.ContainsKey(mockConsumer.Name))
                        {
                            var c = consumers[mockConsumer.Name];
                            Unassign(mockConsumer);
                            c.Topics.Clear();
                        }
                    }

                    #endregion

                    #region Metadata

                    public Metadata GetClusterMetadata()
                    {
                        Metadata metadata = new Metadata(
                            new BrokerMetadata(1, "dummy", 1234).ToSingle().ToList(),
                            topics.Select(t => new TopicMetadata(
                                t.Key,
                                t.Value.Partitions.Select(p =>
                                    new PartitionMetadata(
                                        p.Index,
                                        1,
                                        new int[1] { 1 },
                                        new int[1] { 1 },
                                        new Error(ErrorCode.NoError))).ToList(),
                                new Error(ErrorCode.NoError))).ToList(),
                            1, "1");
                        return metadata;
                    }

                    #endregion

                    #region Partitions Gesture

                    internal List<TopicPartitionOffset> Comitted(MockConsumer mockConsumer)
                    {
                        var c = consumers[mockConsumer.Name];
                        List<TopicPartitionOffset> list = new List<TopicPartitionOffset>();
                        foreach (var p in c.Partitions)
                        {
                            var offset = c.TopicPartitionsOffset.FirstOrDefault(t => t.Topic.Equals(p.Topic) && t.Partition.Equals(p.Partition));
                            if (offset != null)
                            {
                                list.Add(new TopicPartitionOffset(new TopicPartition(p.Topic, p.Partition), new Offset(offset.OffsetComitted)));
                            }
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
                        {
                            yield return new TopicPartition(topic, i);
                        }
                    }

                    private void NeedRebalance2(string groupId)
                    {
                        var partitionsToRevoked =
                            new Dictionary<MockConsumerInformation, List<TopicPartitionOffset>>();
                        var partitionsToAssigned =
                            new Dictionary<MockConsumerInformation, List<TopicPartition>>();

                        foreach (var group in consumerGroups.Keys)
                        {
                            if (group.Equals(groupId))
                            {
                                var topicsByConsumers = new Dictionary<string, List<MockConsumerInformation>>();
                                var consumerPerPartitions = new Dictionary<MockConsumerInformation, List<TopicPartition>>();
                                var _consumers = consumerGroups[group];

                                foreach (var c in _consumers)
                                {
                                    var consumer = consumers[c];
                                    foreach (var t in consumer.Topics)
                                    {
                                        if (topicsByConsumers.ContainsKey(t))
                                            topicsByConsumers[t].Add(consumer);
                                        else
                                            topicsByConsumers.Add(t, new List<MockConsumerInformation>() {consumer});
                                    }
                                }

                                if (consumers.Values
                                        .Where(c => c.GroupId.Equals(group))
                                        .Any(c => !c.Assigned))
                                    // If one consumer in this group is unassigned, need a rebalance
                                {
                                    foreach (var topic in topicsByConsumers.Keys)
                                    {
                                        var topicPartitionNumber = topics[topic].PartitionNumber;
                                        var numberConsumerSub = topicsByConsumers[topic].Count;
                                        int numbPartEach = topicPartitionNumber / numberConsumerSub;
                                        int modulo = topicPartitionNumber % numberConsumerSub;

                                        int j = 0, i = 0;

                                        foreach (var consumer in topicsByConsumers[topic])
                                        {
                                            List<TopicPartition> parts = null;
                                            if (i == numberConsumerSub - 1)
                                                parts = new List<TopicPartition>(Generate(topic, j, numbPartEach + modulo));
                                            else
                                                parts = new List<TopicPartition>(Generate(topic, j, numbPartEach));

                                            if (consumerPerPartitions.ContainsKey(consumer))
                                                consumerPerPartitions[consumer].AddRange(parts);
                                            else
                                                consumerPerPartitions.Add(consumer, parts);

                                            ++i;
                                            j += numbPartEach;
                                        }
                                    }

                                    foreach (var consumer in consumerPerPartitions.Keys)
                                    {
                                        List<TopicPartition> consParts = consumerPerPartitions[consumer];
                                        if (consumer.Partitions.Count > 0) // already partitions assigned, maybe conflicts
                                        {
                                            List<TopicPartition> oldParts = new List<TopicPartition>();
                                            oldParts.AddRange(consumer.Partitions);

                                            // 1 cas : ce sont les memes qu'assignés, et que personne d'autres ne les a => RAF
                                            // Si quelque d'autres les cas, on doit les lui revoked
                                            // 2 cas : Nouvelle partitions dans le lot, assigned
                                            // 3 cas : partitions qui ne sont plus à lui, revoked

                                            foreach (var part in consParts)
                                            {
                                                if (consumer.Partitions.Contains(part) &&
                                                    ContainsPartitionsExceptConsumer(partitionsToAssigned, consumer, part))
                                                {
                                                    if (!partitionsToRevoked.ContainsKey(consumer))
                                                        partitionsToRevoked.Add(consumer, new List<TopicPartitionOffset>());

                                                    var tpo = consumer.TopicPartitionsOffset.FirstOrDefault(t =>
                                                        t.TopicPartition.Equals(part));
                                                    partitionsToRevoked[consumer]
                                                        .Add(new TopicPartitionOffset(part, tpo.OffsetConsumed));
                                                    oldParts.Remove(part);
                                                }
                                                else if (!consumer.Partitions.Contains(part))
                                                {
                                                    if (!partitionsToAssigned.ContainsKey(consumer))
                                                        partitionsToAssigned.Add(consumer, new List<TopicPartition>());
                                                    partitionsToAssigned[consumer].Add(part);
                                                    oldParts.Remove(part);
                                                }
                                                else
                                                    oldParts.Remove(part);
                                            }

                                            if (oldParts.Count > 0)
                                            {
                                                if (!partitionsToRevoked.ContainsKey(consumer))
                                                    partitionsToRevoked.Add(consumer, new List<TopicPartitionOffset>());

                                                foreach (var t in oldParts)
                                                {
                                                    var tpo = consumer.TopicPartitionsOffset
                                                        .FirstOrDefault(_t =>
                                                        _t.TopicPartition.Partition.Value.Equals(t.Partition) && _t.TopicPartition.Topic.Equals(t.Topic));
                                                    partitionsToRevoked[consumer]
                                                        .Add(new TopicPartitionOffset(t, tpo.OffsetConsumed));
                                                }

                                                oldParts.Clear();
                                            }
                                        }
                                        else
                                        {
                                            partitionsToAssigned.Add(consumer, consumerPerPartitions[consumer]);
                                        }
                                    }
                                }
                            }
                        }

                        // remove from metadata topicPartition & offsets
                        foreach (var consumerToRevoke in partitionsToRevoked)
                        {
                            foreach (var tpo in consumerToRevoke.Value)
                            {
                                consumerToRevoke.Key.Partitions.Remove(tpo.TopicPartition);
                                consumerToRevoke.Key.TopicPartitionsOffset.Remove(new MockTopicPartitionOffset
                                {
                                    Partition = tpo.Partition.Value,
                                    Topic = tpo.Topic
                                });
                            }
                        }
                        
                        // add to metadata topicPartition & offsets
                        foreach (var consumerToAssigned in partitionsToAssigned)
                        {
                            foreach (var tp in consumerToAssigned.Value)
                            {
                                consumerToAssigned.Key.Partitions.Add(tp);
                                consumerToAssigned.Key.TopicPartitionsOffset.Add(new MockTopicPartitionOffset
                                {
                                    Partition = tp.Partition.Value,
                                    Topic = tp.Topic,
                                    IsPaused = false,
                                    OffsetComitted = 0,
                                    OffsetConsumed = 0
                                });
                                consumerToAssigned.Key.Assigned = true;
                            }
                        }
                        
                        partitionsToRevoked.ForEach(c => c.Key.RebalanceListener?.PartitionsRevoked(c.Key.Consumer, c.Value));
                        partitionsToAssigned.ForEach(c => c.Key.RebalanceListener?.PartitionsAssigned(c.Key.Consumer, c.Value));
                    }

                    private bool ContainsPartitionsExceptConsumer(
                        Dictionary<MockConsumerInformation, List<TopicPartition>> partitionsToAssigned,
                        MockConsumerInformation consumer,
                        TopicPartition part)
                    {
                        foreach (var kv in partitionsToAssigned)
                        {
                            if (!kv.Key.Equals(consumer) && kv.Value.Contains(part))
                                return true;
                        }
                        return false;
                    }

                    private void NeedRebalance()
                    {
                        foreach (var group in consumerGroups.Keys) {
                                var map = new Dictionary<MockConsumerInformation, List<TopicPartition>>();
                                var newPartitions = new Dictionary<MockConsumerInformation, List<TopicPartition>>();
                                var cons = consumerGroups[group];
                                foreach (var c in cons)
                                {
                                    var consumer = consumers[c];
                                    map.Add(consumer, consumer.Partitions);
                                }

                                if (map.Any(kv => !kv.Key.Assigned)) // If one consumer in this group is unassigned
                                {
                                    // get all partitions for rebalancing
                                    var allPartitions = map.SelectMany(kv => kv.Value);
                                    // get all topics from all partitions
                                    var allTopics = allPartitions.Select(t => t.Topic).Distinct();
                                    allTopics = allTopics.Union(map.SelectMany(kv => kv.Key.Topics)).Distinct().ToList();

                                    foreach (var topic in allTopics)
                                    {
                                        var topicPartitionNumber = topics[topic].PartitionNumber;
                                        var numberConsumerSub = map.Count(kv => kv.Key.Topics.Contains(topic));
                                        int numbPartEach = topicPartitionNumber / numberConsumerSub;
                                        int modulo = topicPartitionNumber % numberConsumerSub;

                                        int j = 0, i = 0;

                                        foreach (var consumer in map.Where(kv => kv.Key.Topics.Contains(topic)).Select(kv => kv.Key))
                                        {
                                            List<TopicPartition> parts = null;
                                            if (i == numberConsumerSub - 1)
                                                parts = new List<TopicPartition>(Generate(topic, j, numbPartEach + modulo));
                                            else
                                                parts = new List<TopicPartition>(Generate(topic, j, numbPartEach));

                                            if (newPartitions.ContainsKey(consumer))
                                                newPartitions[consumer].AddRange(parts);
                                            else
                                                newPartitions.Add(consumer, parts);

                                            ++i;
                                            j += numbPartEach;
                                        }
                                    }

                                    foreach (var consumer in map.Select(kv => kv.Key))
                                    {
                                        if (consumer.Partitions.Count > 0)
                                        {
                                            consumer.Partitions.Clear();
                                            var pList = consumer.TopicPartitionsOffset.Select(f =>
                                                new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetComitted)).ToList();
                                            consumer.RebalanceListener?.PartitionsRevoked(consumer.Consumer, pList);
                                        }

                                        if (newPartitions.ContainsKey(consumer))
                                        {
                                            var parts = newPartitions[consumer];
                                            consumer.Partitions.AddRange(parts);
                                            foreach (var k in parts)
                                            {
                                                if (!consumer.TopicPartitionsOffset.Any(m =>
                                                    m.Topic.Equals(k.Topic) && m.Partition.Equals(k.Partition)))
                                                {
                                                    consumer.TopicPartitionsOffset.Add(new MockTopicPartitionOffset
                                                    {
                                                        OffsetComitted = 0,
                                                        OffsetConsumed = 0,
                                                        Partition = k.Partition,
                                                        Topic = k.Topic
                                                    });
                                                }
                                            }

                                            consumer.RebalanceListener?.PartitionsAssigned(consumer.Consumer, consumer.Partitions);
                                            consumer.Assigned = true;
                                        }
                                    }
                                }
                            }
                    }

                    internal void Assign(MockConsumer mockConsumer, IEnumerable<TopicPartition> topicPartitions)
                    {
                        lock (_lock)
                        {
                            foreach (var t in topicPartitions)
                            {
                                CreateTopic(t.Topic);
                            }

                            lock (_lock)
                            {

                                if (!consumers.ContainsKey(mockConsumer.Name))
                                {
                                    var cons = new MockConsumerInformation
                                    {
                                        GroupId = mockConsumer.MemberId,
                                        Name = mockConsumer.Name,
                                        Consumer = mockConsumer,
                                        Topics = new List<string>(topicPartitions.Select(tp => tp.Topic)),
                                        RebalanceListener = mockConsumer.Listener,
                                        Partitions = new List<TopicPartition>(),
                                        TopicPartitionsOffset = new List<MockConsumerInformation.MockTopicPartitionOffset>()
                                    };
                                    consumers.Add(mockConsumer.Name, cons);

                                    if (consumerGroups.ContainsKey(mockConsumer.MemberId))
                                    {
                                        consumerGroups[mockConsumer.MemberId].Add(mockConsumer.Name);
                                    }
                                    else
                                    {
                                        consumerGroups.Add(mockConsumer.MemberId, new List<string> {mockConsumer.Name});
                                    }
                                }


                                var c = consumers[mockConsumer.Name];
                                var oldPartitionsAssigned = new List<TopicPartition>(c.Partitions);
                                if (topicPartitions.Any())
                                    oldPartitionsAssigned.RemoveAll(t => !topicPartitions.Contains(t));
                                
                                if (oldPartitionsAssigned.Count > 0)
                                {
                                    var partRevokedList = c.TopicPartitionsOffset.Select(f =>
                                        new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetComitted)).ToList();
                                    c.RebalanceListener?.PartitionsRevoked(c.Consumer, partRevokedList);
                                    
                                    var otherConsumersInGroup = consumers.Select(kp => kp.Value)
                                        .Where(i => i.GroupId.Equals(mockConsumer.MemberId) &&
                                                    !i.Name.Equals(mockConsumer.Name))
                                        .ToArray();
                                    
                                    if (otherConsumersInGroup.Length > 0)
                                    {
                                        int partperconsumer = oldPartitionsAssigned.Count / otherConsumersInGroup.Length;
                                        int partmodulo = oldPartitionsAssigned.Count % otherConsumersInGroup.Length;
                                        int partToAssigned = oldPartitionsAssigned.Count;

                                        for (int i = 0; i < otherConsumersInGroup.Length; ++i)
                                        {
                                            if (partToAssigned > 0)
                                            {
                                                var take = i == otherConsumersInGroup.Length - 1 || partperconsumer <= 1
                                                    ? partperconsumer + partmodulo
                                                    : partperconsumer;
                                                var parts = oldPartitionsAssigned
                                                    .Skip(partToAssigned - oldPartitionsAssigned.Count)
                                                    .Take(partperconsumer);

                                                partToAssigned -= partperconsumer;
                                                otherConsumersInGroup[i].Partitions.AddRange(parts);
                                                otherConsumersInGroup[i].RebalanceListener?.PartitionsAssigned(
                                                    otherConsumersInGroup[i].Consumer, otherConsumersInGroup[i].Partitions);
                                            }
                                        }
                                    }
                                }

                                c.Partitions.Clear();

                                List<MockConsumerInformation> customerToRebalance = new List<MockConsumerInformation>();

                                foreach (var p in topicPartitions)
                                {
                                    var info = consumers.Select(kp => kp.Value)
                                        .Where(i => i.GroupId.Equals(mockConsumer.MemberId))
                                        .FirstOrDefault(i => i.Partitions.Contains(p));
                                    if (info != null && !customerToRebalance.Contains(info))
                                    {
                                        customerToRebalance.Add(info);
                                    }
                                }

                                foreach (var cus in customerToRebalance)
                                {
                                    var parts = cus.Partitions.Join(topicPartitions, (t) => t, (t) => t, (t1, t2) => t1)
                                        .ToList();
                                    var pList = cus.TopicPartitionsOffset.Select(f =>
                                        new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetComitted)).ToList();
                                    cus.RebalanceListener?.PartitionsRevoked(cus.Consumer, pList);
                                    foreach (var j in parts)
                                    {
                                        cus.Partitions.Remove(j);
                                    }

                                    cus.RebalanceListener?.PartitionsAssigned(cus.Consumer, cus.Partitions);
                                    cus.Assigned = true;
                                }

                                c.Partitions = new List<TopicPartition>(topicPartitions);
                                foreach (var k in topicPartitions)
                                {
                                    if (!c.TopicPartitionsOffset.Any(m =>
                                        m.Topic.Equals(k.Topic) && m.Partition.Equals(k.Partition)))
                                    {
                                        c.TopicPartitionsOffset.Add(new MockTopicPartitionOffset
                                        {
                                            OffsetComitted = 0,
                                            OffsetConsumed = 0,
                                            Partition = k.Partition,
                                            Topic = k.Topic
                                        });
                                    }
                                }

                                c.RebalanceListener?.PartitionsAssigned(c.Consumer, c.Partitions);
                                c.Assigned = true;
                            }
                        }
                    }

                    internal void Unassign(MockConsumer mockConsumer) 
                    {
                        if (consumers.ContainsKey(mockConsumer.Name))
                        {
                            lock (_lock)
                            {
                                var c = consumers[mockConsumer.Name];
                               
                                    var pList = c.TopicPartitionsOffset.Select(f =>
                                        new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetComitted)).ToList();
                                    c.RebalanceListener?.PartitionsRevoked(c.Consumer, pList);

                                    // Rebalance on other consumer in the same group
                                    var otherConsumers = consumerGroups[mockConsumer.MemberId]
                                        .Where(i => consumers.ContainsKey(i)).Select(i => consumers[i])
                                        .Where(i => !i.Name.Equals(mockConsumer.Name)).ToList();
                                    if (otherConsumers.Count > 0)
                                    {
                                        int partEach = c.Partitions.Count / otherConsumers.Count;
                                        int modulo = c.Partitions.Count % otherConsumers.Count;

                                        int j = 0;
                                        for (int i = 0; i < otherConsumers.Count; ++i)
                                        {
                                            List<TopicPartition> parts = null;
                                            if (i == otherConsumers.Count - 1)
                                            {
                                                parts = c.Partitions.GetRange(j, partEach + modulo);
                                            }
                                            else
                                            {
                                                parts = c.Partitions.GetRange(j, partEach);
                                            }

                                            otherConsumers[i].Partitions.AddRange(parts);
                                            foreach (var k in parts)
                                            {
                                                if (!otherConsumers[i].TopicPartitionsOffset.Any(m =>
                                                    m.Topic.Equals(k.Topic) && m.Partition.Equals(k.Partition)))
                                                {
                                                    otherConsumers[i].TopicPartitionsOffset.Add(new MockTopicPartitionOffset
                                                    {
                                                        OffsetComitted = 0,
                                                        OffsetConsumed = 0,
                                                        Partition = k.Partition,
                                                        Topic = k.Topic
                                                    });
                                                }
                                            }

                                            otherConsumers[i].RebalanceListener
                                                ?.PartitionsAssigned(otherConsumers[i].Consumer,
                                                    otherConsumers[i].Partitions);
                                            otherConsumers[i].Assigned = true;
                                            j += partEach;
                                        }
                                    }

                                    c.Partitions.Clear();
                                    c.Assigned = false;
                            }
                        }
                        else
                        {
                            throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
                        }
                    }

                    #endregion

                    #region Consumer (Read + Commit) Gesture

                    internal List<TopicPartitionOffset> Commit(MockConsumer mockConsumer)
                    {
                        if (consumers.ContainsKey(mockConsumer.Name))
                        {
                            var c = consumers[mockConsumer.Name];
                            foreach (var p in c.TopicPartitionsOffset)
                            {
                                p.OffsetComitted = p.OffsetConsumed;
                            }

                            return c.TopicPartitionsOffset.Select(t => new TopicPartitionOffset(new TopicPartition(t.Topic, t.Partition), t.OffsetComitted)).ToList();
                        }
                        else
                        {
                            throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
                        }
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
                        {
                            throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
                        }
                    }

                    internal ConsumeResult<byte[], byte[]> Consume(MockConsumer mockConsumer, TimeSpan timeout)
                    {
                        foreach (var t in mockConsumer.Subscription)
                        {
                            CreateTopic(t);
                        }

                        DateTime dt = DateTime.Now;
                        ConsumeResult<byte[], byte[]> result = null;
                        if (consumers.ContainsKey(mockConsumer.Name))
                        {
                            var c = consumers[mockConsumer.Name];
                            if (!c.Assigned)
                            {
                                lock(_lock)
                                    NeedRebalance2(c.GroupId);
                            }

                            lock (_lock)
                            {
                                bool stop = false;
                                while (result == null && !stop)
                                {
                                    foreach (var p in c.Partitions)
                                    {
                                        if (timeout != TimeSpan.Zero && (dt + timeout) < DateTime.Now)
                                        {
                                            stop = true;
                                            break;
                                        }

                                        var topic = topics[p.Topic];
                                        var offset = c.TopicPartitionsOffset.FirstOrDefault(t =>
                                            t.Topic.Equals(p.Topic) && t.Partition.Equals(p.Partition));
                                        if (offset != null && !offset.IsPaused)
                                        {
                                            var record = topic.GetMessage(p.Partition, offset.OffsetConsumed);
                                            if (record != null)
                                            {
                                                result = new ConsumeResult<byte[], byte[]>
                                                {
                                                    Offset = offset.OffsetConsumed,
                                                    Topic = p.Topic,
                                                    Partition = p.Partition,
                                                    Message = new Message<byte[], byte[]>
                                                        {Key = record.Key, Value = record.Value}
                                                };
                                                ++offset.OffsetConsumed;
                                                break;
                                            }
                                        }
                                    }

                                    if (timeout == TimeSpan.Zero)
                                        stop = true;
                                }
                            }

                            return result;
                        }
                        else
                        {
                            throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
                        }
                    }

                    internal ConsumeResult<byte[], byte[]> Consume(MockConsumer mockConsumer, CancellationToken cancellationToken)
                        => Consume(mockConsumer, TimeSpan.FromSeconds(10));

                    #endregion

                    #region Producer Gesture

                    internal DeliveryReport<byte[], byte[]> Produce(string topic, Message<byte[], byte[]> message)
                    {
                        DeliveryReport<byte[], byte[]> r = new DeliveryReport<byte[], byte[]>();

                        CreateTopic(topic);

                        // TODO : implement hashpartitionumber
                        var i = RandomNumberGenerator.GetInt32(0, topics[topic].PartitionNumber);
                        topics[topic].AddMessage(message.Key, message.Value, i);

                        r.Message = message;
                        r.Partition = i;
                        r.Topic = topic;
                        r.Offset = topics[topic].GetPartition(i).Size - 1;
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
                        if (topics[topicPartition.Topic].PartitionNumber > topicPartition.Partition)
                        {
                            topics[topicPartition.Topic].AddMessage(message.Key, message.Value, topicPartition.Partition);
                            r.Status = PersistenceStatus.Persisted;
                        }
                        else
                        {
                            topics[topicPartition.Topic].CreateNewPartitions(topicPartition.Partition);
                            topics[topicPartition.Topic].AddMessage(message.Key, message.Value, topicPartition.Partition);
                            r.Status = PersistenceStatus.Persisted;
                        }
                        r.Message = message;
                        r.Partition = topicPartition.Partition;
                        r.Topic = topicPartition.Topic;
                        r.Timestamp = new Timestamp(DateTime.Now);
                        r.Error = new Error(ErrorCode.NoError);
                        r.Status = PersistenceStatus.Persisted;
                        r.Offset = topics[topicPartition.Topic].GetPartition(topicPartition.Partition).Size - 1;
                        return r;
                    }

                    #endregion

                    internal MockConsumerInformation GetConsumerInformation(string consumerName)
                    {
                        if (consumers.ContainsKey(consumerName))
                            return consumers[consumerName];
                        else
                            return null;
                    }
                }
            }
