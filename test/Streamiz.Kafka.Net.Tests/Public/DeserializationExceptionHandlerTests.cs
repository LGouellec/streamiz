using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Tests.Public
{

    public class DeserializationExceptionHandlerTests
    {
        [Test]
        public void TopologyTestDriverDeserializationExceptionHandlerContinueTest()
        {
            var builder = new StreamBuilder();
            var _return = new List<KeyValuePair<string, string>>();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("k", "1234")); // exception in serdes, value.length % 2 == 0
            data.Add(KeyValuePair.Create("v", "123"));
            data.Add(KeyValuePair.Create("t", "12")); // exception in serdes, value.length % 2 == 0

            builder.Stream<string, string>("topic")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));

            var config = new StreamConfig<SerdesThrowException, SerdesThrowException>();
            config.ApplicationId = "test-deserialization-handler";
            config.DeserializationExceptionHandler = (p, r, e) => ExceptionHandlerResponse.CONTINUE;

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTopic = driver.CreateInputTopic<string, string>("topic");
                inputTopic.PipeInputs(data);

                var expected = new List<KeyValuePair<string, string>>();
                expected.Add(KeyValuePair.Create("v", "123"));

                Assert.AreEqual(expected, _return);
            }
        }

        [Test]
        public void TopologyTestDriverDeserializationExceptionHandlerFailTest()
        {
            var builder = new StreamBuilder();
            var _return = new List<KeyValuePair<string, string>>();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("k", "1234")); // exception in serdes, value.length % 2 == 0

            builder.Stream<string, string>("topic");

            var config = new StreamConfig<SerdesThrowException, SerdesThrowException>();
            config.ApplicationId = "test-deserialization-handler";
            config.DeserializationExceptionHandler = (p, r, e) => ExceptionHandlerResponse.FAIL;

            Topology t = builder.Build();

            Assert.Throws<DeserializationException>(() =>
           {
               using (var driver = new TopologyTestDriver(t, config))
               {
                   var inputTopic = driver.CreateInputTopic<string, string>("topic");
                   inputTopic.PipeInputs(data);
               }
           });
        }


        [Test]
        public async Task KafkaStreamDeserializationExceptionHandlerContinueTest()
        {
            var _return = new List<KeyValuePair<string, string>>();

            var timeout = TimeSpan.FromSeconds(10);

            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<SerdesThrowException, SerdesThrowException>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.DeserializationExceptionHandler += (c, r, e) => ExceptionHandlerResponse.CONTINUE;
            config.OffsetCheckpointManager = new MockOffsetCheckpointManager();

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
                        Value = serdes.Serialize("test", new SerializationContext()),
                        Timestamp = new Confluent.Kafka.Timestamp(dt)
                    });
                producer.Produce("test",
                       new Confluent.Kafka.Message<byte[], byte[]>
                       {
                           Key = serdes.Serialize("v", new SerializationContext()),
                           Value = serdes.Serialize("test2", new SerializationContext()),
                           Timestamp = new Confluent.Kafka.Timestamp(dt)
                       });
                Thread.Sleep(1000);
                var expected = new List<KeyValuePair<string, string>>();
                expected.Add(KeyValuePair.Create("v", "test2"));

                Assert.AreEqual(expected, _return);
            }

            stream.Dispose();
        }


        [Test]
        public void KafkaStreamDeserializationExceptionHandlerFailTest()
        {
            bool errorState = false;
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<SerdesThrowException, SerdesThrowException>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);

            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.DeserializationExceptionHandler += (c, r, e) => ExceptionHandlerResponse.FAIL;


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
