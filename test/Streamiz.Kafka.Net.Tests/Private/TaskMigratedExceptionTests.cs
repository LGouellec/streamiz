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

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class TaskMigratedExceptionTests
    {
        #region Inner class

        internal class ProducerSyncExceptionOptions
        {
            public bool IsFatal { get; set; } = false;
            public bool IsRecoverable { get; set; } = false;
            public bool IsProductionException { get; set; } = false;
        }

        internal class ProducerSyncException : SyncKafkaSupplier
        {
            private KafkaProducerException producerException = null;
            private readonly ProducerSyncExceptionOptions options = null;

            public ProducerSyncException(ProducerSyncExceptionOptions options)
            {
                this.options = options;
            }

            public override IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
            {
                if (producerException == null)
                {
                    var p = base.GetProducer(config) as SyncProducer;
                    producerException = new KafkaProducerException(p, options);
                }

                return producerException;
            }
        }

        internal class KafkaProducerException : IProducer<byte[], byte[]>
        {
            private SyncProducer innerProducer;
            private ProducerSyncExceptionOptions options;
            private bool handleError = true;

            public KafkaProducerException(SyncProducer syncProducer)
            {
                this.innerProducer = syncProducer;
            }

            public KafkaProducerException(SyncProducer syncProducer, ProducerSyncExceptionOptions options)
                : this(syncProducer)
            {
                this.options = options;
            }

            public Handle Handle => throw new NotImplementedException();

            public string Name => "";

            public void AbortTransaction(TimeSpan timeout)
            {
            }
            
            public void SetSaslCredentials(string username, string password)
            {
            
            }

            public int AddBrokers(string brokers)
            {
                return 0;
            }

            public void BeginTransaction()
            {
            }

            public void CommitTransaction(TimeSpan timeout)
            {
            }

            public void Dispose()
            {
            }

            public int Flush(TimeSpan timeout)
            {
                return 0;
            }

            public void Flush(CancellationToken cancellationToken = default)
            {
            }

            public void InitTransactions(TimeSpan timeout)
            {
            }

            public int Poll(TimeSpan timeout)
            {
                return 0;
            }

            private void HandleError(DeliveryReport<byte[], byte[]> initReport, Action<DeliveryReport<byte[], byte[]>> deliveryHandler)
            {
                handleError = false;
                if (options.IsProductionException)
                {
                    var result = new DeliveryResult<byte[], byte[]>
                    {
                        Message = initReport.Message,
                        Partition = initReport.Partition,
                        Topic = initReport.Topic
                    };
                    
                    if (options.IsRecoverable)
                    {
                        throw new ProduceException<byte[], byte[]>(new Error(ErrorCode.TransactionCoordinatorFenced,
                            "TransactionCoordinatorFenced", false), result);
                    }
                    else
                    {
                        throw new ProduceException<byte[], byte[]>(
                            new Error(ErrorCode.Local_InvalidArg, "Invalid arg", false),
                            result);
                    }
                }
                else
                {
                    if (options.IsFatal)
                    {
                        initReport.Error = new Error(ErrorCode.TopicAuthorizationFailed, "TopicAuthorizationFailed",
                            true);
                        deliveryHandler(initReport);
                    }
                    else if (options.IsRecoverable)
                    {
                        initReport.Error = new Error(ErrorCode.TransactionCoordinatorFenced,
                            "TransactionCoordinatorFenced",
                            false);
                        deliveryHandler(initReport);
                    }
                    else
                    {
                        initReport.Error = new Error(ErrorCode.Local_InvalidArg, "Invalid arg", false);
                        deliveryHandler(initReport);
                    }
                }
            }

            public void Produce(string topic, Message<byte[], byte[]> message,
                Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
            {
                if (topic == "test" || !handleError)
                    innerProducer.Produce(topic, message, deliveryHandler);
                else
                {
                    var report = new DeliveryReport<byte[], byte[]>
                    {
                        Message = message,
                        Topic = topic
                    };
                    HandleError(report, deliveryHandler);
                }
            }

            public void Produce(TopicPartition topicPartition, Message<byte[], byte[]> message,
                Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
            {
                if (topicPartition.Topic == "test" || !handleError)
                    innerProducer.Produce(topicPartition, message, deliveryHandler);
                else
                {
                    var report = new DeliveryReport<byte[], byte[]>
                    {
                        Message = message,
                        Topic = topicPartition.Topic,
                        Partition = topicPartition.Partition
                    };
                    HandleError(report, deliveryHandler);
                }
            }

            public async Task<DeliveryResult<byte[], byte[]>> ProduceAsync(string topic,
                Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
            {
                if (topic == "test")
                    return await innerProducer.ProduceAsync(topic, message, cancellationToken);
                else
                    throw new NotImplementedException();
            }

            public async Task<DeliveryResult<byte[], byte[]>> ProduceAsync(TopicPartition topicPartition,
                Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
            {
                if (topicPartition.Topic == "test")
                    return await innerProducer.ProduceAsync(topicPartition, message, cancellationToken);
                else
                    throw new NotImplementedException();
            }

            public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets,
                IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
            {
                throw new NotImplementedException();
            }

            public void CommitTransaction()
            {
                throw new NotImplementedException();
            }

            public void AbortTransaction()
            {
                throw new NotImplementedException();
            }
        }

        #endregion

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

            var options = new ProducerSyncExceptionOptions {IsFatal = true};
            var supplier = new ProducerSyncException(options);

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
        
            var options = new ProducerSyncExceptionOptions {IsRecoverable = true};
            var supplier = new ProducerSyncException(options);
        
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
        
            var options = new ProducerSyncExceptionOptions {IsRecoverable = true, IsProductionException = true};
            var supplier = new ProducerSyncException(options);
        
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

            var options = new ProducerSyncExceptionOptions {IsRecoverable = false, IsProductionException = true};
            var supplier = new ProducerSyncException(options);

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

            var options = new ProducerSyncExceptionOptions();
            var supplier = new ProducerSyncException(options);

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

            var options = new ProducerSyncExceptionOptions() {
                IsProductionException = true
            };
            var supplier = new ProducerSyncException(options);

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