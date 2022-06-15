using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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

    }
}