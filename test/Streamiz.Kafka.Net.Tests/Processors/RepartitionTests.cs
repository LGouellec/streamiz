using System.Collections.Generic;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class RepartitionTests
    {
        [Test]
        public void RepartitionTest1()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-repartition-processor"
            };
            
            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .Map((k, v) => KeyValuePair.Create(k.ToUpper(), v))
                .GroupByKey()
                .Count()
                .ToStream()
                .To("output");

            Topology t = builder.Build();

            // TODO : repartition topic task in sync mode
            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                var outputTopic = driver.CreateOuputTopic<string, string>("output");
                inputTopic.PipeInput("test", "test1");
                inputTopic.PipeInput("test", "test2");
                inputTopic.PipeInput("test", "test3");
                var records = outputTopic.ReadKeyValuesToMap();
                Assert.IsNotNull(records);
                Assert.AreEqual(3, records.Count);
                Assert.AreEqual("test-coucou", records["test"]);
                Assert.AreEqual("test2-", records["test2"]);
            }
        }

    }
}