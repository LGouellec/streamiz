using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.TestDriver
{
    public class TopologyTestDriverPartitioningTests
    {
        private static readonly string[] Keys = Enumerable.Range(0, 40).Select(i => $"key-{i}").ToArray();

        private static StreamConfig<StringSerDes, StringSerDes> Config(string applicationId = "test-partitioning")
            => new() { ApplicationId = applicationId };

        private static Topology PassThrough()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input").To("output");
            return builder.Build();
        }

        // input -> select a new key -> count by key (repartition topic) -> output
        private static Topology CountByNewKey()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .SelectKey((_, v, _) => v)
                .GroupByKey()
                .Count(InMemory.As<string, long>("count-store"))
                .ToStream()
                .MapValues((_, v, _) => v.ToString())
                .To("output");
            return builder.Build();
        }

        private static int Murmur2Partition(string key, int partitions)
            => (KafkaPartitioner.Murmur2(Encoding.UTF8.GetBytes(key)) & 0x7fffffff) % partitions;

        #region Partition counts

        [Test]
        public void DefaultIsOnePartition()
        {
            using var driver = new TopologyTestDriver(PassThrough(), Config());
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            input.PipeInputs(Keys.Select(k => new KeyValuePair<string, string>(k, k)));

            var records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(Keys.Length, records.Count);
            Assert.IsTrue(records.All(r => r.Partition.Value == 0));
            CollectionAssert.AreEqual(Enumerable.Range(0, Keys.Length).Select(i => (long)i), records.Select(r => r.Offset.Value));
            Assert.AreEqual(1, driver.GetPartitionCount("input"));
            Assert.AreEqual(1, driver.GetPartitionCount("output"));
        }

        [TestCase(TopologyTestDriver.Mode.SYNC_TASK)]
        [TestCase(TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY)]
        public void ConfiguredPartitionCounts(TopologyTestDriver.Mode mode)
        {
            var partitions = new TestTopicPartitions(defaultPartitions: 2).WithPartitions("input", 3);
            using var driver = new TopologyTestDriver(PassThrough(), Config(), partitions, mode);

            Assert.AreEqual(3, driver.GetPartitionCount("input"));
            Assert.AreEqual(2, driver.GetPartitionCount("output"));
            Assert.AreEqual(2, driver.GetPartitionCount("not-used"));
        }

        [Test]
        public void InvalidPartitionCounts()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new TestTopicPartitions(0));
            Assert.Throws<ArgumentOutOfRangeException>(() => new TestTopicPartitions().WithPartitions("topic", -1));
            Assert.Throws<ArgumentException>(() => new TestTopicPartitions().WithPartitions("", 2));

            var partitions = new TestTopicPartitions(3).WithPartitions("a", 1).WithPartitions("a", 4);
            Assert.AreEqual(3, partitions.DefaultPartitions);
            Assert.AreEqual(4, partitions.Topics["a"]);
        }

        #endregion

        #region Routing

        [TestCase(TopologyTestDriver.Mode.SYNC_TASK)]
        [TestCase(TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY)]
        public void RecordsAreRoutedLikeTheProducer(TopologyTestDriver.Mode mode)
        {
            using var driver = new TopologyTestDriver(PassThrough(), Config(), new TestTopicPartitions(4), mode);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output", TimeSpan.FromSeconds(1));

            input.PipeInputs(Keys.Select(k => new KeyValuePair<string, string>(k, k)));

            var records = mode == TopologyTestDriver.Mode.SYNC_TASK
                ? output.ReadKeyValueList().ToList()
                : IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, Keys.Length);

            Assert.AreEqual(Keys.Length, records.Count);
            foreach (var record in records)
            {
                // murmur2_random is the default partitioner of the stream configuration
                var expected = Murmur2Partition(record.Message.Key, 4);
                Assert.AreEqual(expected, driver.GetPartition("input", record.Message.Key));
                Assert.AreEqual(expected, record.Partition.Value, record.Message.Key);
            }

            // records spread over the partitions, with an offset per partition
            Assert.Greater(records.Select(r => r.Partition.Value).Distinct().Count(), 1);
            foreach (var partition in records.GroupBy(r => r.Partition.Value))
                CollectionAssert.AreEquivalent(Enumerable.Range(0, partition.Count()).Select(i => (long)i), partition.Select(r => r.Offset.Value));

            PartitioningAssert.KeysOnSinglePartition(records);
            PartitioningAssert.PartitionedByKey(driver, records);
        }

        [Test]
        public void ConfiguredPartitionerIsUsed()
        {
            var config = Config();
            config.Partitioner = Partitioner.Consistent;
            using var driver = new TopologyTestDriver(PassThrough(), config, new TestTopicPartitions(5));
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            input.PipeInputs(Keys.Select(k => new KeyValuePair<string, string>(k, k)));

            foreach (var record in output.ReadKeyValueList())
            {
                var expected = (int)(KafkaPartitioner.Crc32(Encoding.UTF8.GetBytes(record.Message.Key)) % 5);
                Assert.AreEqual(expected, record.Partition.Value);
                Assert.AreEqual(expected, driver.GetPartition("output", record.Message.Key));
            }
        }

        [Test]
        public void ProcessorSeesThePartitionOfTheRecord()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapValues((_, _, context) => context.Partition.ToString())
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config(), new TestTopicPartitions(4));
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            input.PipeInputs(Keys.Select(k => new KeyValuePair<string, string>(k, k)));

            foreach (var record in output.ReadKeyValueList())
                Assert.AreEqual(driver.GetPartition("input", record.Message.Key).ToString(), record.Message.Value);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void PipeToAnExplicitPartition(bool reshuffleEveryKey)
        {
            var config = Config();
            config.DefaultPartitionerReshuffleEveryKey = reshuffleEveryKey;
            using var driver = new TopologyTestDriver(PassThrough(), config, new TestTopicPartitions(4));
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            Assert.AreNotEqual(3, driver.GetPartition("output", "key"));
            input.PipeInput(new TestRecord<string, string> { Key = "key", Value = "value", Partition = 3 });
            var record = output.ReadKeyValue();

            // by default, the sink partition is recomputed from the key; without reshuffle, the input partition is kept
            Assert.AreEqual(reshuffleEveryKey ? driver.GetPartition("output", "key") : 3, record.Partition.Value);

            var e = Assert.Throws<ArgumentOutOfRangeException>(() =>
                input.PipeInput(new TestRecord<string, string> { Key = "key", Value = "value", Partition = 4 }));
            StringAssert.Contains("4 partition(s)", e.Message);
        }

        [Test]
        public void NullKeysAreRandomlyPartitioned()
        {
            using var driver = new TopologyTestDriver(PassThrough(), Config(), new TestTopicPartitions(4));
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            Assert.AreEqual(Partition.Any.Value, driver.GetPartition<string>("output", null));

            input.PipeInputs(Enumerable.Range(0, 20).Select(i => new KeyValuePair<string, string>(null, $"v{i}")));
            var records = output.ReadKeyValueList().ToList();

            Assert.AreEqual(20, records.Count);
            Assert.IsTrue(records.All(r => r.Partition.Value is >= 0 and < 4));
            // ignored by the key assertions
            PartitioningAssert.PartitionedByKey(driver, records);
            PartitioningAssert.KeysOnSinglePartition(records);
        }

        #endregion

        #region Repartition

        [TestCase(TopologyTestDriver.Mode.SYNC_TASK)]
        [TestCase(TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY)]
        public void RepartitionAfterKeyChange(TopologyTestDriver.Mode mode)
        {
            var partitions = new TestTopicPartitions(defaultPartitions: 2).WithPartitions("input", 4);
            using var driver = new TopologyTestDriver(CountByNewKey(), Config(), partitions, mode);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output", TimeSpan.FromSeconds(1));

            PartitioningAssert.RepartitionTopicCount(driver, 1);
            var repartition = driver.GetRepartitionTopics().Single();
            StringAssert.StartsWith("test-partitioning-", repartition);
            // a repartition topic is sized like the source topics of the sub-topology writing to it
            Assert.AreEqual(4, driver.GetPartitionCount(repartition));

            // the same value (new key) comes from several input keys, so from several input partitions
            var inputs = Keys.Select((k, i) => new KeyValuePair<string, string>(k, $"group-{i % 5}")).ToList();
            input.PipeInputs(inputs);

            var records = mode == TopologyTestDriver.Mode.SYNC_TASK
                ? output.ReadKeyValueList().ToList()
                : IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, inputs.Count);

            var last = records.GroupBy(r => r.Message.Key).ToDictionary(g => g.Key, g => g.Last().Message.Value);
            Assert.AreEqual(5, last.Count);
            Assert.IsTrue(last.Values.All(v => v == "8"));

            // the count of a key is computed on a single partition of the repartition topic
            PartitioningAssert.KeysOnSinglePartition(records);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void KeyChangeWithoutRepartition(bool reshuffleEveryKey)
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .Map((k, v, _) => KeyValuePair.Create(v, k))
                .To("output");

            var config = Config();
            config.DefaultPartitionerReshuffleEveryKey = reshuffleEveryKey;
            using var driver = new TopologyTestDriver(builder.Build(), config, new TestTopicPartitions(4));
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            input.PipeInputs(Keys.Select(k => new KeyValuePair<string, string>(k, "new-" + k)));
            var records = output.ReadKeyValueList().ToList();

            if (reshuffleEveryKey)
            {
                // the sink recomputes the partition from the new key
                PartitioningAssert.PartitionedByKey(driver, records);
            }
            else
            {
                // the sink keeps the partition of the input record: the new keys are not partitioned by key
                var e = Assert.Throws<PartitioningAssertionException>(() => PartitioningAssert.PartitionedByKey(driver, records));
                StringAssert.Contains("not partitioned by their key", e.Message);
            }

            var error = Assert.Throws<PartitioningAssertionException>(() => PartitioningAssert.RepartitionTopicCount(driver, 1));
            StringAssert.Contains("Expected 1 repartition topic(s), found 0", error.Message);
            PartitioningAssert.RepartitionTopicCount(driver, 0);
        }

        [Test]
        public void ExplicitRepartitionPartitionCount()
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .Repartition(Repartitioned<string, string>.As("resized").WithNumberOfPartitions(6))
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config(), new TestTopicPartitions(2));
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, string>("output");

            var repartition = driver.GetRepartitionTopics().Single();
            Assert.AreEqual(6, driver.GetPartitionCount(repartition));
            Assert.AreEqual(repartition, driver.DescribeTopology().RepartitionTopics.Single());

            input.PipeInputs(Keys.Select(k => new KeyValuePair<string, string>(k, k)));
            var records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(Keys.Length, records.Count);
            PartitioningAssert.KeysOnSinglePartition(records);
        }

        #endregion

        #region Co-partitioning

        private static Topology JoinStreamTable()
        {
            var builder = new StreamBuilder();
            var table = builder.Table("right", InMemory.As<string, string>("right-store"));
            builder.Stream<string, string>("left")
                .Join(table, (l, r) => $"{l}-{r}")
                .To("output");
            return builder.Build();
        }

        [Test]
        public void JoinInputsCoPartitioned()
        {
            var partitions = new TestTopicPartitions(4);
            using var driver = new TopologyTestDriver(JoinStreamTable(), Config(), partitions);
            var left = driver.CreateInputTopic<string, string>("left");
            var right = driver.CreateInputTopic<string, string>("right");
            var output = driver.CreateOutputTopic<string, string>("output");

            CollectionAssert.AreEqual(new[] { "left", "right" }, driver.GetCoPartitionedTopicGroups().Single());
            PartitioningAssert.CoPartitioned(driver);
            PartitioningAssert.CoPartitioned(driver, "left", "right", "output");

            right.PipeInputs(Keys.Select(k => new KeyValuePair<string, string>(k, "r")));
            left.PipeInputs(Keys.Select(k => new KeyValuePair<string, string>(k, "l")));

            var records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(Keys.Length, records.Count);
            Assert.IsTrue(records.All(r => r.Message.Value == "l-r"));
            PartitioningAssert.PartitionedByKey(driver, records);
        }

        [Test]
        public void JoinInputsNotCoPartitioned()
        {
            var partitions = new TestTopicPartitions(4).WithPartitions("right", 2);
            using var driver = new TopologyTestDriver(JoinStreamTable(), Config(), partitions);

            var e = Assert.Throws<PartitioningAssertionException>(() => PartitioningAssert.CoPartitioned(driver));
            StringAssert.Contains("left (4 partitions)", e.Message);
            StringAssert.Contains("right (2 partitions)", e.Message);

            Assert.Throws<PartitioningAssertionException>(() => PartitioningAssert.CoPartitioned(driver, "output", "right"));
            PartitioningAssert.CoPartitioned(driver, "left", "output");
        }

        [Test]
        public void JoinAfterKeyChangeUsesRepartitionTopic()
        {
            var builder = new StreamBuilder();
            var table = builder.Table("right", InMemory.As<string, string>("right-store"));
            builder.Stream<string, string>("left")
                .SelectKey((_, v, _) => v)
                .Join(table, (l, r) => $"{l}-{r}")
                .To("output");

            using var driver = new TopologyTestDriver(builder.Build(), Config(), new TestTopicPartitions(3));

            var group = driver.GetCoPartitionedTopicGroups().Single();
            Assert.AreEqual(2, group.Count);
            CollectionAssert.Contains(group, "right");
            CollectionAssert.Contains(group, driver.GetRepartitionTopics().Single());
            PartitioningAssert.CoPartitioned(driver);
        }

        #endregion

        #region Assertions

        [Test]
        public void KeysOnSinglePartitionFails()
        {
            var records = new List<ConsumeResult<string, string>>
            {
                new() { Topic = "t", Partition = 0, Message = new Message<string, string> { Key = "a" } },
                new() { Topic = "t", Partition = 1, Message = new Message<string, string> { Key = "a" } },
                new() { Topic = "t", Partition = 1, Message = new Message<string, string> { Key = "b" } },
                // same key on another topic: not compared
                new() { Topic = "u", Partition = 2, Message = new Message<string, string> { Key = "b" } }
            };

            var e = Assert.Throws<PartitioningAssertionException>(() => PartitioningAssert.KeysOnSinglePartition(records));
            StringAssert.Contains("key 'a' is on partitions 0, 1 of topic t", e.Message);
            StringAssert.DoesNotContain("'b'", e.Message);

            PartitioningAssert.KeysOnSinglePartition(records.Skip(1));
            PartitioningAssert.KeysOnSinglePartition(
                new[] { records[0], records[1] }.Select(r => new ConsumeResult<string, string>
                {
                    Topic = r.Topic, Partition = r.Partition,
                    Message = new Message<string, string> { Key = r.Partition.Value == 0 ? "A" : "a" }
                }),
                StringComparer.Ordinal);
            Assert.Throws<PartitioningAssertionException>(() => PartitioningAssert.KeysOnSinglePartition(
                new[] { "A", "a" }.Select((k, i) => new ConsumeResult<string, string>
                {
                    Topic = "t", Partition = i, Message = new Message<string, string> { Key = k }
                }),
                StringComparer.OrdinalIgnoreCase));
        }

        [Test]
        public void NullArguments()
        {
            using var driver = new TopologyTestDriver(PassThrough(), Config());
            Assert.Throws<ArgumentNullException>(() => PartitioningAssert.KeysOnSinglePartition<string, string>(null));
            Assert.Throws<ArgumentNullException>(() => PartitioningAssert.PartitionedByKey<string, string>(null, new List<ConsumeResult<string, string>>()));
            Assert.Throws<ArgumentNullException>(() => PartitioningAssert.PartitionedByKey<string, string>(driver, null));
            Assert.Throws<ArgumentNullException>(() => PartitioningAssert.CoPartitioned(null));
            Assert.Throws<ArgumentNullException>(() => PartitioningAssert.RepartitionTopicCount(null, 0));
        }

        [Test]
        public void GetPartitionWithKeySerdes()
        {
            using var driver = new TopologyTestDriver(PassThrough(), Config(), new TestTopicPartitions(7));
            var key = 123456;
            var expected = (KafkaPartitioner.Murmur2(new Int32SerDes().Serialize(key, new SerializationContext())) & 0x7fffffff) % 7;
            Assert.AreEqual(expected, driver.GetPartition("output", key, new Int32SerDes()));
        }

        #endregion
    }
}
