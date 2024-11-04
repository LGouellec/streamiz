using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.TestDriver;

public class Issue382Tests
{
    private class Tag
    {
        public string Field1 { get; set; }
        public string Field2 { get; set; }
        public string Field3 { get; set; }
    }
    
    [Test]
    public void Reproducer()
    {
        List<Tag> Aggregator(string key, Tag value, List<Tag> aggValues)
        {
            if(!aggValues.Contains(value))
                aggValues.Add(value);
            return aggValues;
        }
        
        var streamConfig = new StreamConfig<StringSerDes, JsonSerDes<Tag>>();
        streamConfig.ApplicationId = "test-reproducer-issue382";

        TimeSpan _windowSizeMs = TimeSpan.FromSeconds(5);
        
        var builder = new StreamBuilder();
        var materializer
            = InMemoryWindows
                .As<string, List<Tag>>("tags-agg-store", _windowSizeMs)
                .WithKeySerdes(new StringSerDes())
                .WithValueSerdes(new JsonSerDes<List<Tag>>())
                .WithRetention(_windowSizeMs);

        builder.Stream<string, Tag>("tags")
            .GroupByKey()
            .WindowedBy(TumblingWindowOptions.Of(_windowSizeMs))
            .Aggregate(() => new List<Tag>(), Aggregator, materializer);


        var topology = builder.Build();

        using var driver = new TopologyTestDriver(topology, streamConfig, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);
        var inputTopic = driver.CreateInputTopic<string, Tag, StringSerDes, JsonSerDes<Tag>>("tags");
        bool @continue = true;

        var task = Task.Factory.StartNew(() => { 
            while(@continue)
            {
                DateTime now = DateTime.Now;
                var windowStore = driver.GetWindowStore<string, List<Tag>>("tags-agg-store");
                var items = windowStore?.FetchAll(
                        now.Subtract(_windowSizeMs),
                        now.Add(_windowSizeMs))
                    .ToList();
            }
        });

        var taskProducer = Task.Factory.StartNew(() =>
        {
            DateTime now = DateTime.Now;
            var rd = new Random();
            while (now.AddSeconds(20) > DateTime.Now)
            {
                inputTopic.PipeInput(
                    $"key{rd.NextInt64(1000)}",
                    new Tag {
                        Field1 = $"tag{rd.NextInt64(500)}",
                        Field2 = $"tag{rd.NextInt64(500)}",
                        Field3 = $"tag{rd.NextInt64(500)}"
                    });
            }
        });

        taskProducer.Wait();
        @continue = false;
        task?.Wait();
        
        Assert.IsTrue(driver.IsRunning);
        Assert.IsFalse(driver.IsError);
    }
}