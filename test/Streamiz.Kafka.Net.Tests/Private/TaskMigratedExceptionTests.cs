using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class TaskMigratedExceptionTests
    {
        [Test]
        public void ProductionExceptionFatalHandlerFailTest()
        {
            bool errorState = false;
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);

            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.ProductionExceptionHandler += (r) => ProductionExceptionHandlerResponse.FAIL;

            var options = new ProducerSyncExceptionOptions()
            {
                IsFatal = true,
                NumberOfError = 1,
                WhiteTopics = new List<string> {"test"}
            };
            var supplier = new ProducerSyncExceptionSupplier(options);

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .To("test-output");

            builder.Stream<string, string>("test-output")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));

            var t = builder.Build();

            using (var driver = new TopologyTestDriver(t.Builder, config,
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier))
            {
                var inputtopic = driver.CreateInputTopic<string, string>("test");
                inputtopic.PipeInput("coucou");
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

        [Test]
        public void ProductionExceptionRecoverableHandlerFailTestWithParallel()
        {
            ProductionExceptionRecoverableHandlerFailTest(true);
        }

        [Test]
        public void ProductionExceptionRecoverableHandlerFailTestWithoutParallel()
        {
            ProductionExceptionRecoverableHandlerFailTest(false);
        }
        
        private void ProductionExceptionRecoverableHandlerFailTest(bool parallelProcessing)
        {
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);
            
        
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.ProductionExceptionHandler += (r) => ProductionExceptionHandlerResponse.FAIL;
            config.ParallelProcessing = parallelProcessing;
        
            var options = new ProducerSyncExceptionOptions()
            {
                IsRecoverable = true,
                NumberOfError = 1,
                WhiteTopics = new List<string> {"test"}
            };
            var supplier = new ProducerSyncExceptionSupplier(options);
        
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .To("test-output");
        
            builder.Stream<string, string>("test-output")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));
        
            var t = builder.Build();
        
            using (var driver = new TopologyTestDriver(t.Builder, config,
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier))
            {
                var inputtopic = driver.CreateInputTopic<string, string>("test");
                var outputTopic = driver.CreateOuputTopic<string, string>("test-output");
                inputtopic.PipeInput("coucou");
                inputtopic.PipeInput("coucou");
                while (_return.Count == 0) ;
                var expected = new List<KeyValuePair<string, string>>();
                expected.Add(KeyValuePair.Create<string, string>(null, "coucou"));
                Assert.AreEqual(expected, _return);
            }
        }

        [Test]
        public void ProduceExceptionRecoverableHandlerFailTestWithoutParallel()
        {
            ProduceExceptionRecoverableHandlerFailTest(false);
        }
        
        [Test] public void ProduceExceptionRecoverableHandlerFailTestWithParallel()
        {
            ProduceExceptionRecoverableHandlerFailTest(true);
        }
        
        private void ProduceExceptionRecoverableHandlerFailTest(bool parallelProcessing)
        {
            var errorState = false;
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(100000);
        
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.ProductionExceptionHandler += (r) => ProductionExceptionHandlerResponse.FAIL;
            config.ParallelProcessing = parallelProcessing;
        
            var options = new ProducerSyncExceptionOptions()
            {
                IsRecoverable = true,
                IsProductionException = true,
                NumberOfError = 1,
                WhiteTopics = new List<string> {"test"}
            };
            
            var supplier = new ProducerSyncExceptionSupplier(options);
        
            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .To("test-output");
        
            builder.Stream<string, string>("test-output")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));
        
            var t = builder.Build();
        
            using (var driver = new TopologyTestDriver(t.Builder, config,
                       TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier))
            {
                var inputtopic = driver.CreateInputTopic<string, string>("test");
                var outputTopic = driver.CreateOuputTopic<string, string>("test-output");
                inputtopic.PipeInput("coucou");
                while (_return.Count == 0)
                {
                    Thread.Sleep(100);
                    if (DateTime.Now > dt + timeout)
                    {
                        break;
                    }
                }
        
                var expected = new List<KeyValuePair<string, string>>();
                expected.Add(KeyValuePair.Create<string, string>(null, "coucou"));
                Assert.AreEqual(expected, _return);
            }
        }

        [Test]
        public void ProduceExceptionNotRecoverableHandlerFailTest()
        {
            bool errorState = false;
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);

            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.ProductionExceptionHandler += (r) => ProductionExceptionHandlerResponse.FAIL;

            var options = new ProducerSyncExceptionOptions()
            {
                IsRecoverable = false,
                IsProductionException = true,
                NumberOfError = 1,
                WhiteTopics = new List<string> {"test"}
            };
            
            var supplier = new ProducerSyncExceptionSupplier(options);

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .To("test-output");

            builder.Stream<string, string>("test-output")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));

            var t = builder.Build();

            using (var driver = new TopologyTestDriver(t.Builder, config,
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier))
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
        }


        [Test]
        public void ProduceDeliveryRetryFailTest()
        {
            bool errorState = false;
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);

            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.ProductionExceptionHandler += 
                (_) => ProductionExceptionHandlerResponse.RETRY;

            var options = new ProducerSyncExceptionOptions()
            {
                NumberOfError = 1,
                WhiteTopics = new List<string> {"test"}
            };
            
            var supplier = new ProducerSyncExceptionSupplier(options);

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .To("test-output");

            builder.Stream<string, string>("test-output")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));

            var t = builder.Build();

            using var driver = new TopologyTestDriver(t.Builder, config,
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier);
            var inputtopic = driver.CreateInputTopic<string, string>("test");
            inputtopic.PipeInput("coucou1");
            inputtopic.PipeInput("coucou2");
                
            while (_return.Count == 0)
            {
                Thread.Sleep(100);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
        
            var expected = new List<KeyValuePair<string, string>>();
            expected.Add(KeyValuePair.Create<string, string>(null, "coucou1"));
            expected.Add(KeyValuePair.Create<string, string>(null, "coucou2"));
            Assert.AreEqual(expected, _return);
        }
        
        [Test]
        public void ProduceExceptionRetryFailTest()
        {
            bool errorState = false;
            var _return = new List<KeyValuePair<string, string>>();
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            var dt = DateTime.Now;
            var timeout = TimeSpan.FromSeconds(10);

            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;
            config.ProductionExceptionHandler += 
                (_) => ProductionExceptionHandlerResponse.RETRY;
            
            var options = new ProducerSyncExceptionOptions()
            {
                NumberOfError = 2,
                IsProductionException = true,
                WhiteTopics = new List<string> {"test"}
            };
            
            var supplier = new ProducerSyncExceptionSupplier(options);

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .To("test-output");

            builder.Stream<string, string>("test-output")
                .Peek((k, v) => _return.Add(KeyValuePair.Create(k, v)));

            var t = builder.Build();

            using var driver = new TopologyTestDriver(t.Builder, config,
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY, supplier);
            var inputtopic = driver.CreateInputTopic<string, string>("test");
            inputtopic.PipeInput("coucou1");
            inputtopic.PipeInput("coucou2");
                
            while (_return.Count == 0)
            {
                Thread.Sleep(100);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
        
            var expected = new List<KeyValuePair<string, string>>();
            expected.Add(KeyValuePair.Create<string, string>(null, "coucou1"));
            expected.Add(KeyValuePair.Create<string, string>(null, "coucou2"));
            Assert.AreEqual(expected, _return);
        }

    }
}