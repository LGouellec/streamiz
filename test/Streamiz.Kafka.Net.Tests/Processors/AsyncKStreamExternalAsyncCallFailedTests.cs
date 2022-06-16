using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class AsyncKStreamExternalAsyncCallFailedTests
    {
        private TopologyTestDriver BuildTopology(Action<StreamBuilder> funcBuilder, Action<StreamConfig> configAction = null)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = $"test-external-call-{Guid.NewGuid().ToString()}";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 10;
            config.MaxPollRecords = 10;
            config.CommitIntervalMs = 100;
            config.MetricsRecording = MetricsRecordingLevel.INFO;
            configAction?.Invoke(config);
            
            var builder = new StreamBuilder();
            funcBuilder(builder);

            var topo = builder.Build();

            return new TopologyTestDriver(topo, config, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);
        }

        [Test]
        public void AsyncCallWithRetryWithoutExceptionConfigurable()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ExternalCallAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(
                                new KeyValuePair<string, string>(record.Key, record.Value.ToUpper()));
                        })
                    .To("output");
            }, (config) => config.InnerExceptionHandler = (e) => ExceptionHandlerResponse.FAIL);
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("key1", "value1");
            Thread.Sleep(500);
            Assert.IsTrue(driver.IsError);
        }
        
        [Test]
        public void AsyncCallNotEnoughTimeBufferedException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ExternalCallAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }
                            return await Task.FromResult(
                                new KeyValuePair<string, string>(record.Key, record.Value.ToUpper()));
                        }, 
                        RetryPolicyBuilder
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.BUFFERED)
                            .Build())
                    .To("output");
            }, (conf) => conf.MaxPollIntervalMs = 100);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            input.PipeInput("key1", "value1");
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 1);
            Assert.AreEqual(results[0].Message.Key, "key1");
            Assert.AreEqual(results[0].Message.Value, "VALUE1");
        }

        [Test]
        public void AsyncCallNotEnoughTimeSkipException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ExternalCallAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }
                            return await Task.FromResult(
                                new KeyValuePair<string, string>(record.Key, record.Value.ToUpper()));
                        }, 
                        RetryPolicyBuilder
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.SKIP)
                            .Build())
                    .To("output");
            }, (conf) => conf.MaxPollIntervalMs = 100);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            input.PipeInput("key1", "value1");
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 1, TimeSpan.FromSeconds(1));
            Assert.AreEqual(0, results.Count);
        }
        
        [Test]
        public void AsyncCallNotEnoughTimeFailedException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ExternalCallAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }
                            return await Task.FromResult(
                                new KeyValuePair<string, string>(record.Key, record.Value.ToUpper()));
                        }, 
                        RetryPolicyBuilder
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.FAIL)
                            .Build())
                    .To("output");
            }, (conf) => conf.MaxPollIntervalMs = 100);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            input.PipeInput("key1", "value1");
            Thread.Sleep(1000);
            Assert.IsTrue(driver.IsError);
        }

        [Test]
        public void AsyncCallBufferFull()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ExternalCallAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(
                                new KeyValuePair<string, string>(record.Key, record.Value.ToUpper()));
                        }, 
                        RetryPolicyBuilder
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .NumberOfRetry(1)
                            .RetryBackOffMs(1)
                            .MemoryBufferSize(5)
                            .RetryBehavior(EndRetryBehavior.BUFFERED)
                            .Build())
                    .To("output");
            });
            
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            
            for(int i = 0 ; i < 15 ; ++i)
                input.PipeInput("key1", "value1");
            
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 15);
            Assert.AreEqual(15, results.Count);
        }

        [Test]
        public void AsyncCallInnerExceptionHandler()
        {
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ExternalCallAsync(
                        (_, _) => throw new StreamsException("Exception"));
            }, 
                config => config.InnerExceptionHandler = _ => ExceptionHandlerResponse.CONTINUE);
            
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            
            for(int i = 0 ; i < 5 ; ++i)
                input.PipeInput("key1", "value1");
            
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 5, TimeSpan.FromSeconds(2));
            Assert.AreEqual(0, results.Count);
            Assert.IsFalse(driver.IsError);
        }
    }
}