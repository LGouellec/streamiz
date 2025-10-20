using System;
using System.Collections.Generic;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class CustomTimestampExtractorTicksDateTimeTests
    {
        #region inner class

        internal class ObjectATimestampUnixExtractor : ITimestampExtractor
        {
            public long Extract(ConsumeResult<object, object> record, long partitionTime)
            {
                if (record.Message.Value is ObjectA)
                {
                    ObjectA a = record.Message.Value as ObjectA;
                    return Confluent.Kafka.Timestamp.DateTimeToUnixTimestampMs(a.Date);
                }
                else
                {
                    return partitionTime;
                }
            }
        }

        internal class ObjectATimestampTicksExtractor : ITimestampExtractor
        {
            public long Extract(ConsumeResult<object, object> record, long partitionTime)
            {
                if (record.Message.Value is ObjectA)
                {
                    ObjectA a = record.Message.Value as ObjectA;
                    return a.Date.Ticks;
                }
                else
                {
                    return partitionTime;
                }
            }
        }

        internal class ObjectA
        {
            public string Symbol { get; set; }
            public DateTime Date { get; set; }
        }

        internal class ObjectB
        {
            public DateTime LastDate { get; set; }
            public int Count { get; set; } = 0;
        }

        internal static class ObjectBHelper
        {
            public static ObjectB CreateObjectB(string key, ObjectA a, ObjectB b)
            {
                ObjectB newB = new ObjectB();
                newB.Count = b.Count + 1;
                newB.LastDate = a.Date;
                return newB;
            }
        }

        #endregion

        #region helper methods

        private void BuildTopology(StreamBuilder builder)
        {
            builder.Stream<string, ObjectA, StringSerDes, JsonSerDes<ObjectA>>("source")
                .Map((key, value, _) => new KeyValuePair<string, ObjectA>(value.Symbol, value))
                .GroupByKey<StringSerDes, JsonSerDes<ObjectA>>()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(5)))
                .Aggregate(
                    () => new ObjectB(),
                    (key, ObjectA, ObjectB) => ObjectBHelper.CreateObjectB(key, ObjectA, ObjectB),
                    InMemoryWindows.As<string, ObjectB>("agg-store")
                        .WithKeySerdes(new StringSerDes())
                        .WithValueSerdes(new JsonSerDes<ObjectB>()))
                .ToStream()
                .Map((key, ObjectB, _) => new KeyValuePair<string, ObjectB>(key.Key, ObjectB))
                .To<StringSerDes, JsonSerDes<ObjectB>>("sink");
        }

        private void AssertUseCase(TopologyTestDriver driver)
        {
            var inputTopic = driver.CreateInputTopic<String, ObjectA, StringSerDes, JsonSerDes<ObjectA>>("source");
            var outputTopic = driver.CreateOutputTopic<String, ObjectB, StringSerDes, JsonSerDes<ObjectB>>("sink");
            var dt = DateTime.Parse("2021-04-17T09:21:00-0000");
            var dt2 = dt.AddMinutes(1);
            inputTopic.PipeInput("key1", new ObjectA {Date = dt, Symbol = "$"});
            inputTopic.PipeInput("key1", new ObjectA {Date = dt2, Symbol = "$"});

            var output = outputTopic.ReadKeyValuesToMap();
            Assert.AreEqual(1, output.Count);
            Assert.IsTrue(output.ContainsKey("$"));
            Assert.AreEqual(2, output["$"].Count);
            Assert.AreEqual(dt2, output["$"].LastDate);
        }

        #endregion

        [Test]
        public void UnixTimestampMsTest()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-fix-73";
            config.DefaultTimestampExtractor = new ObjectATimestampUnixExtractor();

            StreamBuilder builder = new StreamBuilder();
            BuildTopology(builder);

            using (var driver = new TopologyTestDriver(builder.Build(), config))
            {
                AssertUseCase(driver);
            }
        }

        [Test]
        public void TicksTestWithParallel() 
        {
            TicksTest(true);
        }

        [Test]
        public void TicksTestWithoutParallel()
        {
            TicksTest(false);
        }

        private void TicksTest(bool parallelProcessing)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-fix-73",
                DefaultTimestampExtractor = new ObjectATimestampTicksExtractor(),
                ParallelProcessing = parallelProcessing
            };

            var builder = new StreamBuilder();
            BuildTopology(builder);

            using var driver = new TopologyTestDriver(builder.Build(), config);
            Assert.Throws<ArgumentOutOfRangeException>(() => AssertUseCase(driver));
        }
    }
}