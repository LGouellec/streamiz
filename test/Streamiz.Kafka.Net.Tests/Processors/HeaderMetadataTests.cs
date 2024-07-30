using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Processors;

public class HeaderMetadataTests
{
    [Test]
    public void TestIssue338Sync()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "test-topic-splitter",
            SchemaRegistryUrl = "mock://test",
            FollowMetadata = true,
            MaxPollIntervalMs = 100
        };
        Headers Headers = new();
        Headers.Add("MessageType", Encoding.ASCII.GetBytes("headers-type"));
        
        var builder = new StreamBuilder();
        builder
            .Stream<string, string>("topic")
            .MapValues((v) =>
            {
                var h = StreamizMetadata.GetCurrentHeadersMetadata();
                h.Add("header1", Encoding.UTF8.GetBytes("value1"));
                return v;
            })
            .To("output");

        using var driver = new TopologyTestDriver(builder.Build(), config, TopologyTestDriver.Mode.SYNC_TASK);
        var inputTopic = driver.CreateInputTopic<string, string>("topic");
        var outputTopic = driver.CreateOuputTopic<string, string>("output");
        
        inputTopic.PipeInput("test-1", "order", Headers);
        
        var sinkRecord = outputTopic.ReadKeyValue();
        Assert.AreEqual(2, sinkRecord.Message.Headers.Count);
        Assert.IsNotNull( sinkRecord.Message.Headers.GetLastBytes("header1"));
        Assert.IsNotNull( sinkRecord.Message.Headers.GetLastBytes("MessageType"));
    }
    
    [Test]
    public void TestIssue338Async()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "test-topic-splitter",
            SchemaRegistryUrl = "mock://test",
            FollowMetadata = true,
            MaxPollIntervalMs = 100
        };
        Headers Headers = new();
        Headers.Add("MessageType", Encoding.ASCII.GetBytes("headers-type"));
        
        var builder = new StreamBuilder();
        builder
            .Stream<string, string>("topic")
            .MapValues((v) =>
            {
                var h = StreamizMetadata.GetCurrentHeadersMetadata();
                h.Add("header1", Encoding.UTF8.GetBytes("value1"));
                return v;
            })
            .To("output");

        using var driver = new TopologyTestDriver(builder.Build(), config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);
        var inputTopic = driver.CreateInputTopic<string, string>("topic");
        var outputTopic = driver.CreateOuputTopic<string, string>("output");
        
        inputTopic.PipeInput("test-1", "order", Headers);
        
        var sinkRecord = outputTopic.ReadKeyValue();
        Assert.AreEqual(2, sinkRecord.Message.Headers.Count);
        Assert.IsNotNull( sinkRecord.Message.Headers.GetLastBytes("header1"));
        Assert.IsNotNull( sinkRecord.Message.Headers.GetLastBytes("MessageType"));
    }
}