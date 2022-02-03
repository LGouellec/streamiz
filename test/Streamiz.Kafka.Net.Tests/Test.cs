using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics;
using System.Threading;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests
{
    public class Test
    {
        [Test]
        public void Test1()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>()
            {
                ApplicationId = "test-reduction",
                BootstrapServers = "localhost:19092"
            };

            StreamBuilder builder = new StreamBuilder();

            builder
                .Stream<string, string>("topic")
                .Filter((key, value) => key == "1")
                .To("tempTopic");

            builder.Stream<string, string>("tempTopic")
                .GroupByKey()
                .Reduce((v1,v2) => v1 + " " + v2)
                .ToStream()
                .To("finalTopic");

            var topology = builder.Build();

            KafkaStream stream = new KafkaStream(topology, config);
            stream.Start();
            bool b = true;
            while (b)
            {
                Thread.Sleep(100);
            }
            stream.Dispose();

           /* using (var driver = new TopologyTestDriver(topology, config))
            {
                var input = driver.CreateInputTopic<string, string>("myTopic");
                var output1 = driver.CreateOuputTopic<string, string>("tempTopic");
                var output = driver.CreateOuputTopic<string, string>("finalTopic");

                input.PipeInput("1", "Once");
                input.PipeInput("2", "Once");
                input.PipeInput("1", "Twice");
                input.PipeInput("3", "Once");
                input.PipeInput("1", "Thrice");
                input.PipeInput("2", "Twice");

                var list12 = output1.ReadKeyValuesToMap();
                var list = output.ReadKeyValueList().Select(r => KeyValuePair.Create(r.Message.Key, r.Message.Value)).ToList();

                foreach (var item in list)
                {
                    Console.WriteLine(item);
                }

                Assert.IsNotNull("x");
            }*/
        }
        
        [Test]
        public void Test2()
        {
            var consumerConfig = new ConsumerConfig();
            consumerConfig.GroupId = "test-consume";
            consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            consumerConfig.BootstrapServers = "localhost:19092";
            List<String> messages = new List<string>();
            
            ConsumerBuilder<String, String> builder = new ConsumerBuilder<string, string>(consumerConfig);
            using (var consumer = builder.Build())
            {
                consumer.Subscribe(new List<string>{"topic", "tempTopic"});
                bool b = true;
                while (b)
                {
                    var record = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (record != null)
                        messages.Add($"{record.Message.Key}-{record.Message.Value}-{record.TopicPartitionOffset}");
                }
            }
        }

    }
}