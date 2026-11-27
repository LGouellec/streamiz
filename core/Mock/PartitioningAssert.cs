using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Mock
{
    /// <summary>
    /// Thrown by <see cref="PartitioningAssert"/> when the partitioning of a topology is not the expected one.
    /// </summary>
    public class PartitioningAssertionException : Exception
    {
        /// <summary>
        /// Create the exception.
        /// </summary>
        /// <param name="message">description of the failed assertion</param>
        public PartitioningAssertionException(string message) : base(message)
        {
        }
    }

    /// <summary>
    /// Assertions on the partitioning of a topology tested with a <see cref="TopologyTestDriver"/>,
    /// independent of the test framework. Each assertion throws a <see cref="PartitioningAssertionException"/> when it fails.
    /// Create the driver with <see cref="TestTopicPartitions"/> to test with several partitions.
    /// </summary>
    public static class PartitioningAssert
    {
        /// <summary>
        /// Assert that all the records with the same key are on the same partition (for each topic).
        /// </summary>
        /// <param name="records">records read from one or several output topics</param>
        /// <param name="keyComparer">key comparer, the default equality comparer of <typeparamref name="K"/> if null</param>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        public static void KeysOnSinglePartition<K, V>(IEnumerable<ConsumeResult<K, V>> records, IEqualityComparer<K> keyComparer = null)
        {
            if (records == null)
                throw new ArgumentNullException(nameof(records));

            var errors = records
                .Where(r => r.Message.Key != null)
                .GroupBy(r => r.Topic)
                .SelectMany(topic => topic
                    .GroupBy(r => r.Message.Key, keyComparer ?? EqualityComparer<K>.Default)
                    .Select(key => (topic: topic.Key, key: key.Key, partitions: key.Select(r => r.Partition.Value).Distinct().OrderBy(p => p).ToList()))
                    .Where(x => x.partitions.Count > 1))
                .Select(x => $"key '{x.key}' is on partitions {string.Join(", ", x.partitions)} of topic {x.topic}")
                .ToList();

            if (errors.Any())
                throw new PartitioningAssertionException(
                    $"Records with the same key must be on the same partition: {string.Join("; ", errors)}");
        }

        /// <summary>
        /// Assert that each record is on the partition a producer would choose for its key
        /// (see <see cref="TopologyTestDriver.GetPartition{K}"/>). It detects records written on the partition of a stale
        /// key, for instance after a <c>Map</c> or a <c>SelectKey</c> without repartition when
        /// <see cref="IStreamConfig.DefaultPartitionerReshuffleEveryKey"/> is false (a sink then keeps the partition of
        /// the input record), or with a custom stream partitioner. Records with a randomly partitioned key (null key) are ignored.
        /// </summary>
        /// <param name="driver">test driver</param>
        /// <param name="records">records read from one or several output topics</param>
        /// <param name="keySerdes">key serdes, the default key serdes of the configuration if null</param>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        public static void PartitionedByKey<K, V>(TopologyTestDriver driver, IEnumerable<ConsumeResult<K, V>> records, ISerDes<K> keySerdes = null)
        {
            if (driver == null)
                throw new ArgumentNullException(nameof(driver));
            if (records == null)
                throw new ArgumentNullException(nameof(records));

            var errors = records
                .Select(r => (record: r, expected: driver.GetPartition(r.Topic, r.Message.Key, keySerdes)))
                .Where(x => x.expected != Partition.Any.Value && x.expected != x.record.Partition.Value)
                .Select(x => $"key '{x.record.Message.Key}' is on partition {x.record.Partition.Value} of topic {x.record.Topic} instead of {x.expected}")
                .ToList();

            if (errors.Any())
                throw new PartitioningAssertionException(
                    $"Records are not partitioned by their key: {string.Join("; ", errors)}");
        }

        /// <summary>
        /// Assert that topics are co-partitioned (same number of partitions), as required for the inputs of a join.
        /// Without <paramref name="topics"/>, checks the input topics of every join of the topology
        /// (see <see cref="TopologyTestDriver.GetCoPartitionedTopicGroups"/>).
        /// </summary>
        /// <param name="driver">test driver</param>
        /// <param name="topics">topics that must have the same number of partitions, all the join inputs if empty</param>
        public static void CoPartitioned(TopologyTestDriver driver, params string[] topics)
        {
            if (driver == null)
                throw new ArgumentNullException(nameof(driver));

            var groups = topics != null && topics.Length > 0
                ? new List<IReadOnlyCollection<string>> { topics }
                : driver.GetCoPartitionedTopicGroups();

            var errors = groups
                .Select(g => g.Select(t => (topic: t, partitions: driver.GetPartitionCount(t))).ToList())
                .Where(g => g.Select(t => t.partitions).Distinct().Count() > 1)
                .Select(g => string.Join(", ", g.Select(t => $"{t.topic} ({t.partitions} partitions)")))
                .ToList();

            if (errors.Any())
                throw new PartitioningAssertionException(
                    $"Topics are not co-partitioned: {string.Join("; ", errors)}");
        }

        /// <summary>
        /// Assert the number of repartition topics of the topology, for instance to check that a key-changing operation
        /// followed by a stateful operation or a join creates a repartition topic.
        /// </summary>
        /// <param name="driver">test driver</param>
        /// <param name="expected">expected number of repartition topics</param>
        public static void RepartitionTopicCount(TopologyTestDriver driver, int expected)
        {
            if (driver == null)
                throw new ArgumentNullException(nameof(driver));

            var topics = driver.GetRepartitionTopics();
            if (topics.Count != expected)
                throw new PartitioningAssertionException(
                    $"Expected {expected} repartition topic(s), found {topics.Count}" +
                    (topics.Count > 0 ? $": {string.Join(", ", topics)}" : ""));
        }
    }
}
