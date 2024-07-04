using System;
using System.Linq;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
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
        public Partition Partition(string topic, string key, string value, Partition sourcePartition, int numPartitions)
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

    private class MyTopicNameExtractor : ITopicNameExtractor<string, string>
    {
        public string Extract(string key, string value, IRecordContext recordContext)
            => "output";
    }

    private class MyRecordTimestampExtractor : IRecordTimestampExtractor<string, string>
    {
        private readonly long _ts;

        public MyRecordTimestampExtractor(long ts)
        {
            _ts = ts;
        }

        public long Extract(string key, string value, IRecordContext recordContext)
            => _ts;
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
                (_, _, _,_, _) => new Partition(0));

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(0, record["test1"].Partition.Value);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(0, record["test2"].Partition.Value);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(0, record["test3"].Partition.Value);
    }
    
    [Test]
    public void StreamToLambdaCustomPartitionerWithSerdesTest()
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
                (_, _, _,_, _) => new Partition(0), 
                new StringSerDes(),
                new StringSerDes());

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(0, record["test1"].Partition.Value);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(0, record["test2"].Partition.Value);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(0, record["test3"].Partition.Value);
    }
    
    [Test]
    public void StreamToLambdaCustomPartitionerWithSerdesTypeTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();
        
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To<StringSerDes, StringSerDes>("output", 
                (_, _,_, _, _) => new Partition(0));

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(0, record["test1"].Partition.Value);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(0, record["test2"].Partition.Value);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(0, record["test3"].Partition.Value);
    }
    
    [Test]
    public void StreamToTopicExtractorLambdaCustomPartitionerWithSerdesTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();
        
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To((_,_,_) => "output", 
                (_, _,_, _, _) => new Partition(0), 
                new StringSerDes(),
                new StringSerDes());

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(0, record["test1"].Partition.Value);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(0, record["test2"].Partition.Value);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(0, record["test3"].Partition.Value);
    }
    
    [Test]
    public void StreamToTopicExtractorLambdaCustomPartitionerWithSerdesTypeTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();
        
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To<StringSerDes, StringSerDes>((_,_,_) => "output", 
                (_, _,_, _, _) => new Partition(0));

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(0, record["test1"].Partition.Value);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(0, record["test2"].Partition.Value);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(0, record["test3"].Partition.Value);
    }
    
    [Test]
    public void StreamToTopicExtractorLambdaCustomPartitionerTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();
        
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To((_,_,_) => "output", 
                (_, _,_, _, _) => new Partition(0));

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(0, record["test1"].Partition.Value);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(0, record["test2"].Partition.Value);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(0, record["test3"].Partition.Value);
    }
    
    [Test]
    public void StreamToTopicExtractorWithSerdesTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();
        
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To(new MyTopicNameExtractor(), 
                new StringSerDes(),
                new StringSerDes());

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
    }

    [Test]
    public void StreamToTopicExtractorWithSerdesTypeTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();
        
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To<StringSerDes, StringSerDes>(new MyTopicNameExtractor());

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
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
        
        var record = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(outputTopic, 5)
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
    
    [Test]
    public void StreamToTopicExtractorLambdaCustomPartitionerRecordExtractorTimestampTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var tst = DateTime.Now.GetMilliseconds();
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To((_,_,_) => "output", 
                (_,_,_) => tst,
                (_, _,_, _, _) => new Partition(0));

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(0, record["test1"].Partition.Value);
        Assert.AreEqual(tst, record["test1"].Message.Timestamp.UnixTimestampMs);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(0, record["test2"].Partition.Value);
        Assert.AreEqual(tst, record["test2"].Message.Timestamp.UnixTimestampMs);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(0, record["test3"].Partition.Value);
        Assert.AreEqual(tst, record["test3"].Message.Timestamp.UnixTimestampMs);
    }

    [Test]
    public void StreamToTopicExtractorRecordExtractorTimestampWithSerdesTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var tst = DateTime.Now.GetMilliseconds();
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To((_,_,_) => "output", 
                new StringSerDes(), new StringSerDes(),
                (_,_,_) => tst);

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(tst, record["test1"].Message.Timestamp.UnixTimestampMs);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(tst, record["test2"].Message.Timestamp.UnixTimestampMs);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(tst, record["test3"].Message.Timestamp.UnixTimestampMs);
    }

    [Test]
    public void StreamToTopicExtractorRecordExtractorTimestampWithSerdesTypeTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var tst = DateTime.Now.GetMilliseconds();
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To<StringSerDes, StringSerDes>((_,_,_) => "output", 
                (_,_,_) => tst);

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(tst, record["test1"].Message.Timestamp.UnixTimestampMs);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(tst, record["test2"].Message.Timestamp.UnixTimestampMs);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(tst, record["test3"].Message.Timestamp.UnixTimestampMs);
    }
    
    [Test]
    public void StreamToTopicExtractorRecordExtractorTimestampTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var tst = DateTime.Now.GetMilliseconds();
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To(new MyTopicNameExtractor(),
                new MyRecordTimestampExtractor(tst));

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(tst, record["test1"].Message.Timestamp.UnixTimestampMs);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(tst, record["test2"].Message.Timestamp.UnixTimestampMs);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(tst, record["test3"].Message.Timestamp.UnixTimestampMs);
    }

    [Test]
    public void StreamToTopicExtractorStreamPartitionerTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var tst = DateTime.Now.GetMilliseconds();
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To(new MyTopicNameExtractor(),
                new MyStreamPartitioner());

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.IsTrue(record["test1"].TopicPartition.Partition.Value >= 0);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.IsTrue(record["test2"].TopicPartition.Partition.Value >= 0);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.IsTrue(record["test3"].TopicPartition.Partition.Value >= 0);
    }
    
    [Test]
    public void StreamToTopicExtractorRecordExtractorTimestampStreamPartitionerTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var tst = DateTime.Now.GetMilliseconds();
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To(new MyTopicNameExtractor(),
                new MyRecordTimestampExtractor(tst), 
                new MyStreamPartitioner());

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(tst, record["test1"].Message.Timestamp.UnixTimestampMs);
        Assert.IsTrue(record["test1"].TopicPartition.Partition.Value >= 0);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(tst, record["test2"].Message.Timestamp.UnixTimestampMs);
        Assert.IsTrue(record["test2"].TopicPartition.Partition.Value >= 0);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(tst, record["test3"].Message.Timestamp.UnixTimestampMs);
        Assert.IsTrue(record["test3"].TopicPartition.Partition.Value >= 0);
    }
    
    [Test]
    public void StreamToTopicExtractorRecordExtractorTimestampWithSerdesWrapperTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var tst = DateTime.Now.GetMilliseconds();
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To(new MyTopicNameExtractor(),
                new StringSerDes(), new StringSerDes(),
                new MyRecordTimestampExtractor(tst));

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(tst, record["test1"].Message.Timestamp.UnixTimestampMs);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(tst, record["test2"].Message.Timestamp.UnixTimestampMs);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(tst, record["test3"].Message.Timestamp.UnixTimestampMs);
    }
    
    [Test]
    public void StreamToTopicExtractorRecordExtractorTimestampWithSerdesTypeWrapperTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var tst = DateTime.Now.GetMilliseconds();
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To<StringSerDes, StringSerDes>(new MyTopicNameExtractor(),
                new MyRecordTimestampExtractor(tst));

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(tst, record["test1"].Message.Timestamp.UnixTimestampMs);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(tst, record["test2"].Message.Timestamp.UnixTimestampMs);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(tst, record["test3"].Message.Timestamp.UnixTimestampMs);
    }

    
    [Test]
    public void StreamToLambdaTopicExtractorWithSerdesTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var tst = DateTime.Now.GetMilliseconds();
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To((_, _, _) => "output", new StringSerDes(), new StringSerDes());

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
    }

    [Test]
    public void StreamToLambdaTopicExtractorWithSerdesTypeTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var tst = DateTime.Now.GetMilliseconds();
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To<StringSerDes, StringSerDes>((_, _, _) => "output");

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
    }
    
    [Test]
    public void StreamToAllLambdaWithSerdesTypeTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var tst = DateTime.Now.GetMilliseconds();
        builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper())
            .To<StringSerDes, StringSerDes>(
                (_, _, _) => "output",
                (_,_,_) => tst,
                (_,_,_,_,_) => new Partition(0));

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
        var record = IntegrationTestUtils
            .WaitUntilMinKeyValueRecordsReceived(outputTopic, 3)
            .ToDictionary(c => c.Message.Key, c => c);
        
        Assert.IsNotNull(record);
        Assert.AreEqual(3, record.Count);
        Assert.AreEqual("test1", record["test1"].Message.Key);
        Assert.AreEqual("TEST1", record["test1"].Message.Value);
        Assert.AreEqual(tst, record["test1"].Message.Timestamp.UnixTimestampMs);
        Assert.AreEqual(0, record["test1"].TopicPartition.Partition.Value);
        
        Assert.AreEqual("test2", record["test2"].Message.Key);
        Assert.AreEqual("TEST2", record["test2"].Message.Value);
        Assert.AreEqual(tst, record["test2"].Message.Timestamp.UnixTimestampMs);
        Assert.AreEqual(0, record["test2"].TopicPartition.Partition.Value);
        
        Assert.AreEqual("test3", record["test3"].Message.Key);
        Assert.AreEqual("TEST3", record["test3"].Message.Value);
        Assert.AreEqual(tst, record["test3"].Message.Timestamp.UnixTimestampMs);
        Assert.AreEqual(0, record["test3"].TopicPartition.Partition.Value);
    }
    
    [Test]
    public void StreamArgumentExceptionTopicEmptyTest()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var stream = builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper());

        Assert.Throws<ArgumentException>(() => stream.To(
            string.Empty, (_,_,_,_,_) => Partition.Any));
    }

    [Test]
    public void StreamArgumentExceptionTopicEmpty2Test()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var stream = builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper());

        Assert.Throws<ArgumentException>(() => stream.To(
            string.Empty, new MyStreamPartitioner()));
    }
    
    [Test]
    public void StreamArgumentExceptionTopicEmpty3Test()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var stream = builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper());

        Assert.Throws<ArgumentException>(() => stream.To(
            string.Empty, new StringSerDes(), new StringSerDes()));
    }
    
    [Test]
    public void StreamArgumentExceptionTopicEmpty4Test()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes> {
            ApplicationId = "test-stream-table-left-join"
        };

        StreamBuilder builder = new StreamBuilder();

        var stream = builder
            .Stream<string, string>("stream")
            .MapValues((v) => v.ToUpper());

        Assert.Throws<ArgumentException>(() => stream.To(
            string.Empty, (_,_,_,_,_) => Partition.Any, new StringSerDes(), new StringSerDes()));
    }
}