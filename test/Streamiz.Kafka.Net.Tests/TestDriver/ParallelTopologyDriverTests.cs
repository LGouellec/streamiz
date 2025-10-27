using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.TestDriver;

public class ParallelTopologyDriverTests
{
    private void ProduceInputTopic(int numberThreads,
        int numberOfMessageProducedPerThread,
        TestInputTopic<String, String> testInputTopic)
    {
        Thread[] threads = new Thread[numberThreads];
        
        for (int i = 0; i < numberThreads; i++)
        {
            threads[i] = new Thread(() =>
            {
                for(int j = 0 ; j < numberOfMessageProducedPerThread ;  ++j)
                    testInputTopic.PipeInput($"key{j%10}", $"value{j}");
            });
            threads[i].Start();
        }

        foreach (Thread t in threads)
            t.Join();
    }
    
    [TestCase(TopologyTestDriver.Mode.SYNC_TASK)]
    [TestCase(TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY)]
    [TestCase(TopologyTestDriver.Mode.SYNC_TASK, 10, 6000)]
    [TestCase(TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, 10, 6000)]
    [Test]
    public void ParallelDriverTinyMessages(TopologyTestDriver.Mode mode, int numberThreads = 5, int numRecordPerThread = 200)
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>();
        config.ApplicationId = "test-multiple-thread-driver";

        var builder = new StreamBuilder();

        builder
            .Stream<string, string>("topic")
            .MapValues((v, r) => v.ToUpper())
            .To("output");

        var topology = builder.Build();
        
        using var driver = new TopologyTestDriver(topology, config, mode);
        
        var input = driver.CreateInputTopic<string, string>("topic");
        ProduceInputTopic(numberThreads, numRecordPerThread, input);
        var output = driver.CreateOutputTopic<string, string>("output");
        var records = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, numberThreads * numRecordPerThread,
            TimeSpan.FromMinutes(1));
        Assert.IsNotNull(records);
        Assert.IsTrue(records.Any());
        Assert.AreEqual(numberThreads * numRecordPerThread, records.Count());
    }
}