using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Private
{
    /// <summary>
    /// Tests to verify that records flow correctly through topologies
    /// that use Aggregate → ToStream → Repartition when AdvanceWallClockTime is used.
    /// </summary>
    [TestFixture]
    public class TopologyBuildOrderBugTests
    {
        /// <summary>
        /// Basic test: Aggregate → ToStream → Repartition should forward records.
        /// </summary>
        [Test]
        public void Aggregate_ToStream_Repartition_Should_Forward_Records()
        {
            var builder = new StreamBuilder();

            builder.Stream<string, int>("input")
                .GroupByKey()
                .Aggregate(
                    () => 0,
                    (k, v, agg) => agg + v,
                    Materialized<string, int, IKeyValueStore<Bytes, byte[]>>
                        .Create()
                        .With<StringSerDes, Int32SerDes>())
                .ToStream()
                .Repartition()
                .To("output");

            var config = new StreamConfig<StringSerDes, Int32SerDes>
            {
                ApplicationId = "test-topology-build-order-bug"
            };

            using var driver = new TopologyTestDriver(builder.Build(), config);

            var input = driver.CreateInputTopic<string, int>("input");
            var output = driver.CreateOutputTopic<string, int>("output");

            // Send a record
            input.PipeInput("key1", 100);

            // Advance time to trigger any buffered/punctuation-based processing
            driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(100));

            // Read results - this should have our aggregated value
            var results = output.ReadKeyValuesToMap();

            Assert.That(results.ContainsKey("key1"), Is.True,
                "Records should flow through Aggregate → ToStream → Repartition → To.");
            Assert.That(results["key1"], Is.EqualTo(100));
        }

        /// <summary>
        /// Test with multiple repartitions.
        /// </summary>
        [Test]
        public void Multiple_Repartitions_Should_Forward_Records()
        {
            var builder = new StreamBuilder();

            builder.Stream<string, int>("input")
                .GroupByKey()
                .Aggregate(
                    () => 0,
                    (k, v, agg) => agg + v,
                    Materialized<string, int, IKeyValueStore<Bytes, byte[]>>
                        .Create()
                        .With<StringSerDes, Int32SerDes>())
                .ToStream()
                .Repartition()  // First repartition
                .GroupByKey()
                .Aggregate(
                    () => 0,
                    (k, v, agg) => agg + v,
                    Materialized<string, int, IKeyValueStore<Bytes, byte[]>>
                        .Create("second-agg-store")
                        .With<StringSerDes, Int32SerDes>())
                .ToStream()
                .Repartition()  // Second repartition
                .To("output");

            var config = new StreamConfig<StringSerDes, Int32SerDes>
            {
                ApplicationId = "test-multiple-repartitions"
            };

            using var driver = new TopologyTestDriver(builder.Build(), config);

            var input = driver.CreateInputTopic<string, int>("input");
            var output = driver.CreateOutputTopic<string, int>("output");

            input.PipeInput("key1", 100);

            // Need multiple time advances to flush through multiple stages
            driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(100));
            driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(100));
            driver.AdvanceWallClockTime(TimeSpan.FromMilliseconds(100));

            var results = output.ReadKeyValuesToMap();

            Assert.That(results.ContainsKey("key1"), Is.True,
                "Records should flow through multiple Aggregate → ToStream → Repartition stages.");
            Assert.That(results["key1"], Is.EqualTo(100));
        }
    }
}
