using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Mock
{
    /// <summary>
    /// Number of partitions of the topics used by a <see cref="TopologyTestDriver"/>.
    /// <para>
    /// Input and output topics get <see cref="DefaultPartitions"/> partitions, unless set with <see cref="WithPartitions"/>.
    /// Internal topics (repartition and changelog topics) are sized like in a real application:
    /// a repartition topic gets the maximum number of partitions of the source topics of the sub-topology writing to it
    /// (or <c>Repartitioned.WithNumberOfPartitions</c>), a changelog topic gets the number of partitions of its
    /// sub-topology. Set them explicitly with <see cref="WithPartitions"/> and their real name
    /// (<c>{application.id}-{name}-repartition</c>) to override this.
    /// </para>
    /// <example>
    /// <code>
    /// var partitions = new TestTopicPartitions(defaultPartitions: 4)
    ///     .WithPartitions("orders", 4)
    ///     .WithPartitions("customers", 2);
    /// using var driver = new TopologyTestDriver(topology, config, partitions);
    /// </code>
    /// </example>
    /// </summary>
    public sealed class TestTopicPartitions
    {
        private readonly Dictionary<string, int> topics = new();

        /// <summary>
        /// Create a partitions configuration.
        /// </summary>
        /// <param name="defaultPartitions">number of partitions of the topics not configured with <see cref="WithPartitions"/></param>
        public TestTopicPartitions(int defaultPartitions = 1)
        {
            DefaultPartitions = CheckPositive(defaultPartitions, nameof(defaultPartitions));
        }

        /// <summary>
        /// Number of partitions of the topics not configured with <see cref="WithPartitions"/>.
        /// </summary>
        public int DefaultPartitions { get; }

        /// <summary>
        /// Topics configured with <see cref="WithPartitions"/>, and their number of partitions.
        /// </summary>
        public IReadOnlyDictionary<string, int> Topics => topics;

        /// <summary>
        /// Set the number of partitions of a topic.
        /// </summary>
        /// <param name="topic">topic name</param>
        /// <param name="partitions">number of partitions</param>
        /// <returns>this instance, for chaining</returns>
        public TestTopicPartitions WithPartitions(string topic, int partitions)
        {
            if (string.IsNullOrEmpty(topic))
                throw new ArgumentException("The topic name must be set", nameof(topic));
            topics[topic] = CheckPositive(partitions, nameof(partitions));
            return this;
        }

        private static int CheckPositive(int value, string name)
            => value > 0 ? value : throw new ArgumentOutOfRangeException(name, value, "The number of partitions must be positive");
    }
}
