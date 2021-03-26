using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
namespace Streamiz.Kafka.Net.Tests.Public
{
    public class InnerExceptionHandlerTests
    {
        private class FailTimestampExtractor : ITimestampExtractor
        {
            public long Extract(ConsumeResult<object, object> record, long partitionTime)
            {
                throw new NotImplementedException();
            }
        }

        [Test]
        public async Task KafkaStreamInnerExceptionHandlerContinueTest()
        {
            var _return = new List<KeyValuePair<string, string>>();

            var timeout = TimeSpan.FromSeconds(10);

            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.InnerExceptionHandler += (e) => ExceptionHandlerResponse.CONTINUE;
            config.DefaultTimestampExtractor = new FailTimestampExtractor();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));

            var t = builder.Build();
            var stream = new KafkaStream(t, config, supplier);

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(isRunningState);

            if (isRunningState)
            {
                var serdes = new StringSerDes();
                dt = DateTime.Now;
                producer.Produce("test",
                    new Confluent.Kafka.Message<byte[], byte[]>
                    {
                        Key = serdes.Serialize("k", new SerializationContext()),
                        Value = serdes.Serialize("test4", new SerializationContext()),
                        Timestamp = new Confluent.Kafka.Timestamp(dt)
                    });
                Thread.Sleep(1000);
                var expected = new List<KeyValuePair<string, string>>();
                Assert.AreEqual(expected, _return);
            }

            stream.Dispose();
        }


        [Test]
        public void KafkaStreamInnerExceptionHandlerFailTest()
        {
            bool errorState = false;
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);

            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.InnerExceptionHandler += (e) => ExceptionHandlerResponse.FAIL;
            config.DefaultTimestampExtractor = new FailTimestampExtractor();

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));


            var t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY))
            {
                var inputtopic = driver.CreateInputTopic<string, string>("test");
                inputtopic.PipeInput("coucou");
                while (!errorState)
                {
                    errorState = driver.IsError;
                    Thread.Sleep(10);
                    if (DateTime.Now > dt + timeout)
                    {
                        break;
                    }
                }
                Assert.IsTrue(driver.IsError);
            }

            Assert.AreEqual(new List<KeyValuePair<string, string>>(), _return);
        }
    }
}