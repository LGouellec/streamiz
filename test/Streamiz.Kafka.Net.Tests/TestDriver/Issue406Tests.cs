using System;
using System.Linq;
using System.Text;
using NUnit.Framework;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Mock;

namespace Streamiz.Kafka.Net.Tests.TestDriver;

public class Issue406Tests
{
    static readonly String SUM_OF_ODD_NUMBERS_TOPIC = "sum-of-odd-numbers-topic";
    static readonly String NUMBERS_TOPIC = "numbers-topic";
    static readonly String BACK_UP_NUMBERS_TOPIC = "backup-numbers-topic";

    [Test]
    public void Reproducer()
    {
        var topology = GetTopology();

        var config = new StreamConfig<StringSerDes, StringSerDes>();
        config.ApplicationId = "test-test-driver-app";


        using (var driver = new TopologyTestDriver(topology, config))
        {
            var inputTopic = driver.CreateInputTopic<string, string>(BACK_UP_NUMBERS_TOPIC);
            var otherInputTopic = driver.CreateInputTopic<string, string>(NUMBERS_TOPIC);
            var outputTopic =
                driver.CreateOutputTopic<string, string>(SUM_OF_ODD_NUMBERS_TOPIC, TimeSpan.FromSeconds(5));

            for (var i = 0; i < 10; i++)
            {
                inputTopic.PipeInput((i % 2).ToString(), i.ToString());
            }

            for (var i = 10; i < 20; i++)
            {
                otherInputTopic.PipeInput((i % 2).ToString(), i.ToString());
            }

            var result = outputTopic.ReadValueList().ToList();
            StringBuilder sb = new StringBuilder();
            foreach (var r in result)
                sb.AppendLine(r);
            Assert.AreEqual(20, result.Count);
        }
    }

    static Topology GetTopology()
    {
        StreamBuilder builder = new();

        var numberStream = builder
            .Stream<string, string>(NUMBERS_TOPIC);

        var backupNumberStream = builder
            .Stream<string, string>(BACK_UP_NUMBERS_TOPIC);

        var merged = numberStream.Merge(backupNumberStream);

        var groupedStream = merged.GroupBy((k,v,_) => k);
        //var groupedStream = merged.GroupByKey();

        groupedStream.Aggregate(
                () => "Start",
                (k, v, old) => { return old += v; })
            .ToStream().To<StringSerDes, StringSerDes>(SUM_OF_ODD_NUMBERS_TOPIC);

        return builder.Build();
    }
}