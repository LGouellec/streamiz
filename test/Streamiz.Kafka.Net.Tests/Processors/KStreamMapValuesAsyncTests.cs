using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamMapValuesAsyncTests : AbstractAsyncTests
    {
        public KStreamMapValuesAsyncTests() 
            : base("test-map-values-async")
        {
        }

        [Test]
        public void KStreamMapValuesAsyncWithoutRetryOk()
        {
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapValuesAsync(
                        async (record, _) => await Task.FromResult(record.Value.ToUpper()))
                    .To("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValue();
            AssertKeyValue(result, "key1", "VALUE1");
        }
        
        [Test]
        public void KStreamMapValuesAsyncWithRetryOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToUpper());
                        }, 
                            RetryPolicy
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
            AssertKeyValue(result, "key1", "VALUE1");
        }
        
        [Test]
        public void KStreamMapValuesAsyncWithMultipleRetryOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 3)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToUpper());
                        }, 
                        RetryPolicy
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
            AssertKeyValue(result, "key1", "VALUE1");
        }
        
        [Test]
        public void KStreamMapValuesAsyncRetryAttemptOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToUpper());
                        }, 
                        RetryPolicy
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
        public void KStreamMapValuesAsyncRetryFailed()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToUpper());
                        }, 
                        RetryPolicy
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
        
        [Test]
        public void KStreamMapValuesAsyncWithRetryWithoutExceptionConfigurable()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToUpper());
                        })
                    .To("output");
            }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                (config) => config.InnerExceptionHandler = (e) => ExceptionHandlerResponse.FAIL);
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("key1", "value1");
            Thread.Sleep(500);
            Assert.IsTrue(driver.IsError);
        }
        
        [Test]
        public void KStreamMapValuesAsyncNotEnoughTimeBufferedException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }
                            return await Task.FromResult(record.Value.ToUpper());
                        }, 
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.BUFFERED)
                            .Build())
                    .To("output");
            }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                (conf) => conf.MaxPollIntervalMs = 100);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            input.PipeInput("key1", "value1");
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 1);
            AssertKeyValue(results[0], "key1", "VALUE1");
        }

        [Test]
        public void KStreamMapValuesAsyncNotEnoughTimeSkipException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }
                            return await Task.FromResult(record.Value.ToUpper());
                        }, 
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.SKIP)
                            .Build())
                    .To("output");
            }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                (conf) => conf.MaxPollIntervalMs = 100);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            input.PipeInput("key1", "value1");
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 1, TimeSpan.FromSeconds(1));
            Assert.AreEqual(0, results.Count);
        }
        
        [Test]
        public void KStreamMapValuesAsyncNotEnoughTimeFailedException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }
                            return await Task.FromResult(record.Value.ToUpper());
                        }, 
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.FAIL)
                            .Build())
                    .To("output");
            }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                (conf) => conf.MaxPollIntervalMs = 100);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            input.PipeInput("key1", "value1");
            Thread.Sleep(1000);
            Assert.IsTrue(driver.IsError);
        }

        [Test]
        public void KStreamMapValuesAsyncBufferFull()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .MapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToUpper());
                        }, 
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .NumberOfRetry(1)
                            .RetryBackOffMs(1)
                            .MemoryBufferSize(5)
                            .RetryBehavior(EndRetryBehavior.BUFFERED)
                            .Build())
                    .To("output");
            }, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);
            
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            
            for(int i = 0 ; i < 15 ; ++i)
                input.PipeInput("key1", "value1");
            
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 15);
            Assert.AreEqual(15, results.Count);
        }
        
    }
}