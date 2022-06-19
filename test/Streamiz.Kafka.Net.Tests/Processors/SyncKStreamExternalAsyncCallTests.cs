using System;
using System.Collections.Generic;
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
    public class SyncKStreamExternalAsyncCallTests
    {
        private TopologyTestDriver BuildTopology(Action<StreamBuilder> funcBuilder)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = $"test-external-call-{Guid.NewGuid().ToString()}";
            config.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            config.PollMs = 10;
            config.MaxPollRecords = 10;
            config.CommitIntervalMs = 100;
            config.MetricsRecording = MetricsRecordingLevel.INFO;

            var builder = new StreamBuilder();
            funcBuilder(builder);

            var topo = builder.Build();

            return new TopologyTestDriver(topo, config);
        }

        [Test]
        public void SyncAsyncCallWithoutRetryOk()
        {
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapAsync(
                        async (record, _) => await Task.FromResult(new KeyValuePair<string, string>(record.Key, record.Value.ToUpper())))
                    .To("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValue();
            Assert.AreEqual(result.Message.Key, "key1");
            Assert.AreEqual(result.Message.Value, "VALUE1");
        }
        
        [Test]
        public void SyncAsyncCallWithRetryOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(
                                new KeyValuePair<string, string>(record.Key, record.Value.ToUpper()));
                        }, 
                            RetryPolicyBuilder
                                .NewBuilder()
                                .RetriableException<StreamsException>()
                                .Build())
                    .To("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValue();
            Assert.AreEqual(2, numberCall);
            Assert.AreEqual(result.Message.Key, "key1");
            Assert.AreEqual(result.Message.Value, "VALUE1");
        }
        
        [Test]
        public void SyncAsyncCallWithMultipleRetryOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 3)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(
                                new KeyValuePair<string, string>(record.Key, record.Value.ToUpper()));
                        }, 
                        RetryPolicyBuilder
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .Build())
                    .To("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValue();
            Assert.AreEqual(4, numberCall);
            Assert.AreEqual(result.Message.Key, "key1");
            Assert.AreEqual(result.Message.Value, "VALUE1");
        }
        
        [Test]
        public void SyncAsyncCallRetryAttemptOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapAsync(
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
                            .NumberOfRetry(5)
                            .RetriableException<StreamsException>()
                            .Build())
                    .To("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValue();
            // null because the default behavior for the retry policy is failed
            Assert.IsNull(result);
        }
        
        [Test]
        public void SyncAsyncCallRetryFailed()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapAsync(
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
                            .NumberOfRetry(5)
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.FAIL)
                            .Build())
                    .To("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            Assert.Throws<NoneRetryableException>(() => input.PipeInput("key1", "value1"));
        }
        
        // TODO : add not enougth time exception + async mode (buffered + skip + failed test)
    }
}