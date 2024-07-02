using System;
using System.Linq;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Processors;

public class KStreamToTests
{
    private class MyStreamPartitioner : IStreamPartitioner<string, string>
    {
        public Partition Partition(string topic, string key, string value, int numPartitions)
        {
            switch (key)
            {
                case "order":
                    return new Partition(0);
                case "sale":
                    return new Partition(1);
                default:
                    return Confluent.Kafka.Partition.Any;
            }
        }
    }
    
    [Test]
    public void StreamToLambdaCustomPartitionerTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();
        
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To("output", 
                (_, _, _, _) => new Partition(0));

        Topology t = builder.Build();

        var mockSupplier = new MockKafkaSupplier(4);
        mockSupplier.CreateTopic("output");
        mockSupplier.CreateTopic("stream");
        
        using var driver = new TopologyTestDriver(t.Builder, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, mockSupplier);
        var inputTopic = driver.CreateInputTopic<string, string>("stream");
        var outputTopic = driver.CreateOuputTopic<string, string>("output");
        inputTopic.PipeInput("test1", "test1");
        inputTopic.PipeInput("test2", "test2");
        inputTopic.PipeInput("test3", "test3");
        var record = outputTopic.ReadKeyValueList().ToList();
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record[0].Message.Key);
        Assert.AreEqual("TEST1", record[0].Message.Value);
        Assert.AreEqual(0, record[0].Partition.Value);
            
            
        Assert.AreEqual("test2", record[1].Message.Key);
        Assert.AreEqual("TEST2", record[1].Message.Value);
        Assert.AreEqual(0, record[1].Partition.Value);
            
            
        Assert.AreEqual("test3", record[2].Message.Key);
        Assert.AreEqual("TEST3", record[2].Message.Value);
        Assert.AreEqual(0, record[2].Partition.Value);
    }
    
    [Test]
    public void StreamToCustomPartitionerTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();
        
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To("output", new MyStreamPartitioner());

        Topology t = builder.Build();

        var mockSupplier = new MockKafkaSupplier(4);
        mockSupplier.CreateTopic("output");
        mockSupplier.CreateTopic("stream");
        
        using var driver = new TopologyTestDriver(t.Builder, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, mockSupplier);
        var inputTopic = driver.CreateInputTopic<string, string>("stream");
        var outputTopic = driver.CreateOuputTopic<string, string>("output", TimeSpan.FromSeconds(2));
        inputTopic.PipeInput("order", "order1");
        inputTopic.PipeInput("order", "order2");
        inputTopic.PipeInput("sale", "sale1");
        inputTopic.PipeInput("other", "other");
        inputTopic.PipeInput("test", "test1");
        
        var record = outputTopic.ReadKeyValueList()
            .ToList();

        var orderRecord = record.Where(o => o.Message.Key.Equals("order")).ToList();
        var saleRecord = record.Where(o => o.Message.Key.Equals("sale")).ToList();
        var otherRecord = record.Where(o => o.Message.Key.Equals("other") || o.Message.Key.Equals("test")).ToList();
        
        Assert.IsNotNull(record);
        Assert.AreEqual(5, record.Count);
        
        Assert.AreEqual("order", orderRecord[0].Message.Key);
        Assert.AreEqual("ORDER1", orderRecord[0].Message.Value);
        Assert.AreEqual(0, orderRecord[0].Partition.Value);
        
        
        Assert.AreEqual("order", orderRecord[1].Message.Key);
        Assert.AreEqual("ORDER2", orderRecord[1].Message.Value);
        Assert.AreEqual(0, orderRecord[1].Partition.Value);
            
            
        Assert.AreEqual("sale", saleRecord[0].Message.Key);
        Assert.AreEqual("SALE1", saleRecord[0].Message.Value);
        Assert.AreEqual(1, saleRecord[0].Partition.Value);
        
        Assert.AreEqual(2, otherRecord.Count);
    }
}