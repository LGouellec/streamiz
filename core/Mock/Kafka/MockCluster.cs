using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using log4net;
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
                       ((MockTopicPartitionOffset) obj).Topic.Equals(Topic) &&
                       ((MockTopicPartitionOffset) obj).Partition.Equals(Partition);
            }

            public override int GetHashCode()
            {
                return Topic.GetHashCode() & Partition.GetHashCode() ^ 33333;
            }
        }

        public string GroupId { get; set; }
        public string Name { get; set; }
        public ThreadSafeList<TopicPartition> Partitions { get; set; }
        public List<string> Topics { get; set; }
        public MockConsumer Consumer { get; set; }
        public IConsumerRebalanceListener RebalanceListener { get; set; }
        public bool Assigned { get; set; }

        public List<MockTopicPartitionOffset> TopicPartitionsOffset { get; set; }

        public bool NeedRebalanceTriggered { get; set; }
        public string RebalanceId { get; set; }
        public List<TopicPartitionOffset> PartitionsToRevoked { get; set; }
        public List<TopicPartition> PartitionsToAssigned { get; set; }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            return obj is MockConsumerInformation && ((MockConsumerInformation) obj).Name.Equals(Name);
        }
    }

    internal class MockGroupOffset
    {
        public string GroupId { get; set; }
        public long Offset { get; set; }
        public TopicPartition TopicPartition { get; set; }
    }

    // TODO : refactor all metadata from mock cluster, pause, resume, offsets ...
    internal class MockCluster
    {
        internal readonly int DEFAULT_NUMBER_PARTITIONS;
        private readonly long waitBeforeRebalanceMs;
        private readonly static object _lock = new();
        private readonly ILog log = Logger.GetLogger(typeof(MockCluster));
        
        #region Ctor

        public MockCluster(int defaultNumberPartitions = 1, long waitBeforeRebalanceMs = 0)
        {
            DEFAULT_NUMBER_PARTITIONS = defaultNumberPartitions;
            this.waitBeforeRebalanceMs = waitBeforeRebalanceMs;
            groupCoordinator = new(this);
        }

        public void Destroy()
        {
            topics.Clear();
            consumers.Clear();
            consumerGroups.Clear();
            groupOffsets.Clear();
        }

        #endregion

        private readonly ConcurrentDictionary<string, MockTopic> topics = new();
        private readonly ConcurrentDictionary<string, MockConsumerInformation> consumers = new();
        private readonly ConcurrentDictionary<string, List<string>> consumerGroups = new();
        private readonly ConcurrentDictionary<string, List<MockGroupOffset>> groupOffsets = new();
        private readonly MockGroupCoordinator groupCoordinator = null;

        #region Topic Gesture

        internal void CreateTopic(string topic) => CreateTopic(topic, DEFAULT_NUMBER_PARTITIONS);

        internal void CreateTopic(string topic, int partitions)
        {
            if (!topics.Values.Any(t => t.Name.Equals(topic, StringComparison.InvariantCultureIgnoreCase)))
            {
                var t = new MockTopic(topic, partitions);
                topics.TryAddOrUpdate(topic, t);
                log.Debug($"Created topic {topic} with {partitions} partitions");
            }
        }

        internal void Pause(MockConsumer mockConsumer, IEnumerable<TopicPartition> enumerable)
        {
            lock (_lock)
            {
                var partitions = enumerable.ToList();
                if (consumers.ContainsKey(mockConsumer.Name))
                {
                    foreach (var tpo in consumers[mockConsumer.Name].TopicPartitionsOffset)
                    {
                        if (partitions.Contains(tpo.TopicPartition))
                        {
                            tpo.IsPaused = true;
                            log.Debug($"Consumer {mockConsumer.Name} pause partition {tpo.TopicPartition}");
                        }
                    }
                }
            }
        }

        internal void CloseConsumer(string name)
        {
            if (consumers.ContainsKey(name))
            {
                log.Debug($"Closing consumer {name}");
                Unsubscribe(consumers[name].Consumer);
                consumers[name].Consumer.Subscription.Clear();
                log.Debug($"Consumer {name} close");

            }
        }

        internal void Resume(MockConsumer mockConsumer, IEnumerable<TopicPartition> enumerable)
        {
            lock (_lock)
            {
                var partitions = enumerable.ToList();
                if (consumers.ContainsKey(mockConsumer.Name))
                {
                    foreach (var tpo in consumers[mockConsumer.Name].TopicPartitionsOffset)
                    {
                        if (partitions.Contains(tpo.TopicPartition))
                        {
                            tpo.IsPaused = false;
                            log.Debug($"Consumer {mockConsumer.Name} resume partition {tpo.TopicPartition}");
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
                            log.Debug($"Consumer {mockConsumer.Name} seek offset {tpos.OffsetConsumed} to topic/partition {tpo.TopicPartition}");
                        }
                    }
                }
            }
        }

        internal void SubscribeTopic(MockConsumer consumer, IEnumerable<string> topics)
        {
            lock (_lock)
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
                        Partitions = new(),
                        Assigned = false,
                        TopicPartitionsOffset = new ()
                    };
                    consumers.TryAddOrUpdate(consumer.Name, cons);

                    if (consumerGroups.ContainsKey(consumer.MemberId))
                    {
                        consumerGroups[consumer.MemberId].Add(consumer.Name);
                    }
                    else
                    {
                        consumerGroups.TryAddOrUpdate(consumer.MemberId, new List<string> {consumer.Name});
                    }

                    if (!groupOffsets.ContainsKey(consumer.MemberId))
                        groupOffsets.TryAddOrUpdate(consumer.MemberId, new List<MockGroupOffset>());
                    
                    Thread.Sleep((int)waitBeforeRebalanceMs);
                }
                else
                {
                    throw new StreamsException(
                        $"Client {consumer.Name} already subscribe topic. Please call unsucribe before");
                }
            }
        }

        internal void Unsubscribe(MockConsumer mockConsumer)
        {
            lock (_lock)
            {
                if (consumers.ContainsKey(mockConsumer.Name))
                {
                    var c = consumers[mockConsumer.Name];

                    // 1. remove to consumer group
                    var consumersGroup = consumerGroups[c.GroupId];
                    consumersGroup.Remove(c.Name);

                    // 2. remove consumer metadata
                    consumers.TryRemove(mockConsumer.Name, out MockConsumerInformation m);

                    // 3. set flag assigned = false, other consumer in group for rebalance next time
                    foreach (var _c in consumersGroup)
                        consumers[_c].Assigned = false;
                }
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
                            new int[1] {1},
                            new int[1] {1},
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
                var offset =
                    c.TopicPartitionsOffset.FirstOrDefault(t =>
                        t.Topic.Equals(p.Topic) && t.Partition.Equals(p.Partition));
                if (offset != null)
                {
                    list.Add(new TopicPartitionOffset(new TopicPartition(p.Topic, p.Partition),
                        new Offset(offset.OffsetComitted)));
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

        private void NeedRebalance2(string groupId, string consumerName)
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
                                        ContainsPartitionsExceptConsumer(partitionsToAssigned, consumerPerPartitions, consumer, part))
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
                                                _t.TopicPartition.Partition.Value.Equals(t.Partition) &&
                                                _t.TopicPartition.Topic.Equals(t.Topic));
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

            
            // double check
            VerificationCheckAssignedPartitions(partitionsToAssigned, partitionsToRevoked);

            var rebalanceId =
                groupCoordinator.NewRebalanceTransaction(partitionsToRevoked.Keys.Union(partitionsToAssigned.Keys)
                    .ToList());
            
            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Calculate reassign partitions after this rebalance process : {rebalanceId}");
            sb.AppendLine("Revoked partitions: ");

            // remove from metadata topicPartition & offsets
            foreach (var consumerToRevoke in partitionsToRevoked)
            {
                consumerToRevoke.Key.NeedRebalanceTriggered = true;
                consumerToRevoke.Key.PartitionsToRevoked = consumerToRevoke.Value;
                consumerToRevoke.Key.RebalanceId = rebalanceId;
                sb.AppendLine(
                    $"  {consumerToRevoke.Key.Name}:{string.Join(",", consumerToRevoke.Value.Select(p => $"[{p.Topic}-{p.Partition}]"))}");
            }

            sb.AppendLine($"Assigned partitions: ");
            // add to metadata topicPartition & offsets
            foreach (var consumerToAssigned in partitionsToAssigned)
            {
                consumerToAssigned.Key.NeedRebalanceTriggered = true;
                consumerToAssigned.Key.PartitionsToAssigned = consumerToAssigned.Value;
                consumerToAssigned.Key.RebalanceId = rebalanceId;

                sb.AppendLine(
                    $"  {consumerToAssigned.Key.Name}:{string.Join(",", consumerToAssigned.Value.Select(p => $"[{p.Topic}-{p.Partition}]"))}");
            }
            
            log.Info(sb.ToString());
        }

        internal void Assign2(MockConsumer mockConsumer, IEnumerable<TopicPartition> topicPartitions)
        {
            lock (_lock)
            {
                foreach (var t in topicPartitions)
                    CreateTopic(t.Topic);

                var copyPartitions = new List<TopicPartition>();

                if (!consumers.ContainsKey(mockConsumer.Name))
                {
                    var cons = new MockConsumerInformation
                    {
                        GroupId = mockConsumer.MemberId,
                        Name = mockConsumer.Name,
                        Consumer = mockConsumer,
                        Topics = new (),
                        RebalanceListener = mockConsumer.Listener,
                        Partitions = new(),
                        TopicPartitionsOffset = new ()
                    };
                    consumers.TryAddOrUpdate(mockConsumer.Name, cons);

                    if (consumerGroups.ContainsKey(mockConsumer.MemberId))
                    {
                        consumerGroups[mockConsumer.MemberId].Add(mockConsumer.Name);
                    }
                    else
                    {
                        consumerGroups.TryAddOrUpdate(mockConsumer.MemberId, new List<string> {mockConsumer.Name});
                    }
                }

                var c = consumers[mockConsumer.Name];
                copyPartitions.AddRange(c.Partitions);
                c.Partitions.Clear();
                bool r = CheckConsumerAlreadyAssign(c.GroupId, c.Name, topicPartitions);
                if (r)
                    c.Partitions = new ThreadSafeList<TopicPartition>(copyPartitions);
                else
                {
                    foreach(var tp in topicPartitions)
                        c.Partitions.Add(tp);
                    foreach (var tp in c.Partitions)
                    {
                        var offset = GetGroupOffset(c.GroupId, tp);
                         if (c.TopicPartitionsOffset.Contains(new MockTopicPartitionOffset()
                             {Partition = tp.Partition.Value, Topic = tp.Topic}))
                         {
                             var tpo = c.TopicPartitionsOffset.FirstOrDefault(t =>
                                 t.Partition.Equals(tp.Partition.Value) && t.Topic.Equals(tp.Topic));
                             tpo.OffsetComitted = offset.Offset;
                             tpo.OffsetConsumed = offset.Offset;
                             if (!copyPartitions.Contains(tp))
                                 tpo.IsPaused = false;
                         }
                        else
                            c.TopicPartitionsOffset.Add(new MockTopicPartitionOffset()
                            {
                                Partition = tp.Partition.Value,
                                IsPaused = false,
                                OffsetComitted = offset.Offset,
                                OffsetConsumed = offset.Offset,
                                Topic = tp.Topic
                            });
                    }
                }

                log.Debug($"Consumer {mockConsumer.Name} assigned partitions list : {string.Join(",", c.Partitions)}");
                c.Assigned = true;
            }
        }

        internal void Unassign2(MockConsumer mockConsumer)
        {
            lock (_lock)
            {
                if (consumers.ContainsKey(mockConsumer.Name))
                {
                    consumers[mockConsumer.Name].Partitions.Clear();
                    consumers[mockConsumer.Name].Topics.Clear();
                    consumers[mockConsumer.Name].TopicPartitionsOffset.Clear();
                    log.Debug($"Consumer {mockConsumer.Name} unassigned partitions");
                }
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

                return c.TopicPartitionsOffset.Select(t =>
                    new TopicPartitionOffset(new TopicPartition(t.Topic, t.Partition), t.OffsetComitted)).ToList();
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
                    var p = c.TopicPartitionsOffset.FirstOrDefault(t =>
                        t.Topic.Equals(o.Topic) && t.Partition.Equals(o.Partition));
                    var groupOffset = GetGroupOffset(c.GroupId, o.TopicPartition);
                    if (p != null)
                    {
                        p.OffsetConsumed = o.Offset.Value;
                        p.OffsetComitted = o.Offset.Value;
                        groupOffset.Offset = o.Offset.Value;
                        log.Debug($"Consumer {mockConsumer.Name} commit topic/partition {o.TopicPartition}, offset {o.Offset.Value}");
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
            foreach (var t in mockConsumer.Subscription.ToList())
                CreateTopic(t);

            DateTime dt = DateTime.Now;
            ConsumeResult<byte[], byte[]> result = null;
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                lock (_lock)
                {
                    if (!c.Assigned && !c.NeedRebalanceTriggered)
                        NeedRebalance2(c.GroupId, c.Name);

                    if (!groupCoordinator.TryToCompleteRebalancing(c.RebalanceId, c.Name))
                        return null;
                }

                bool stop = false;
                while (result == null && !stop)
                {
                    foreach (var p in c.Partitions.ToList())
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
                                log.Debug($"Consumer {mockConsumer.Name} consume message from topic/partition {p}, offset {offset.OffsetConsumed}");
                                break;
                            }

                        }
                    }

                    if (timeout == TimeSpan.Zero)
                        stop = true;
                }
                
                return result;
            }

            return null;
        }

        internal ConsumeResult<byte[], byte[]> Consume(MockConsumer mockConsumer, CancellationToken cancellationToken)
            => Consume(mockConsumer, TimeSpan.FromSeconds(1));

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
            return consumers.ContainsKey(consumerName) ? consumers[consumerName] : null;
        }

        internal MockGroupOffset GetGroupOffset(string groupId, TopicPartition topicPartition)
        {
            MockGroupOffset tmp = null;
            if (groupOffsets.ContainsKey(groupId))
            {
                if (groupOffsets[groupId].Any(g => g.TopicPartition.Equals(topicPartition)))
                    tmp = groupOffsets[groupId].First(g => g.TopicPartition.Equals(topicPartition));
                else
                {
                    MockGroupOffset groupOffset = new MockGroupOffset();
                    groupOffset.GroupId = groupId;
                    groupOffset.TopicPartition = topicPartition;
                    groupOffset.Offset = 0; // begin
                    groupOffsets[groupId].Add(groupOffset);
                    tmp = groupOffset;
                }
            }
            else
            {
                MockGroupOffset groupOffset = new MockGroupOffset();
                groupOffset.GroupId = groupId;
                groupOffset.TopicPartition = topicPartition;
                groupOffset.Offset = 0; // begin
                groupOffsets.TryAddOrUpdate(groupId, new List<MockGroupOffset> {groupOffset});
                tmp = groupOffset;
            }
            return tmp;
        }

        private bool CheckConsumerAlreadyAssign(string groupId, string consumerName,
            IEnumerable<TopicPartition> topicPartitions)
        {
            var otherConsumers = consumerGroups[groupId].Where(c => c.Equals(consumerName)).ToList();
            foreach (var otherCons in otherConsumers)
            {
                foreach (var tp in topicPartitions)
                    if (consumers[otherCons].Partitions.Contains(tp))
                        return true;
            }

            return false;
        }

        private bool ContainsPartitionsExceptConsumer(
            Dictionary<MockConsumerInformation, List<TopicPartition>> partitionsToAssigned,
            Dictionary<MockConsumerInformation, List<TopicPartition>> calculatedPartitionsByConsumer,
            MockConsumerInformation consumer,
            TopicPartition part)
        {
            foreach (var kv in partitionsToAssigned)
            {
                if (!kv.Key.Equals(consumer) && kv.Value.Contains(part))
                    return true;
            }
            
            foreach (var kv in calculatedPartitionsByConsumer)
            {
                if (!kv.Key.Equals(consumer) && kv.Value.Contains(part))
                    return true;
            }

            return false;
        }
        
        private void VerificationCheckAssignedPartitions(
            Dictionary<MockConsumerInformation, List<TopicPartition>> partitionsToAssigned,
            Dictionary<MockConsumerInformation, List<TopicPartitionOffset>> partitionsToRevoked)
        {
            foreach (var kvR in partitionsToRevoked)
            {
                if (partitionsToAssigned.ContainsKey(kvR.Key))
                {
                    var partitionsAssigned = partitionsToAssigned[kvR.Key];
                    
                    foreach(var tp in kvR.Value)
                        if (partitionsAssigned.Contains(tp.TopicPartition))
                            partitionsAssigned.Remove(tp.TopicPartition);
                    
                    if (partitionsAssigned.Count == 0)
                        partitionsToAssigned.Remove(kvR.Key);
                }
            }
        }
    }
}