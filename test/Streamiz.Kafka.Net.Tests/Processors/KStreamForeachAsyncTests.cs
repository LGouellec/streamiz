using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamForeachAsyncTests : AbstractAsyncTests
    {
        private IDictionary<string, string> data = null;
        
        public KStreamForeachAsyncTests() 
            : base("test-foreach-async")
        {
        }

        private void AssertKeyValueOnData(string expectedKey, string expectedValue)
        {
            Assert.IsTrue(data.ContainsKey(expectedKey));
            Assert.AreEqual(expectedValue, data[expectedKey]);
        }
        
        [TearDown]
        public void Dispose() => data.Clear();

        [SetUp]
        public void Init()
        {
            data = new Dictionary<string, string>();
        }
        
        [Test]
        public void KStreamForeachAsyncWithoutRetryOk()
        {
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ForeachAsync(
                        async (record, _) =>
                        {
                            data.AddOrUpdate(record.Key, record.Value);
                            await Task.CompletedTask;
                        });
            });
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("key1", "value1");
            AssertKeyValueOnData( "key1", "value1");
        }
        
        [Test]
        public void KStreamForeachAsyncWithRetryOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ForeachAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                                throw new StreamsException("Exception");
                            data.AddOrUpdate(record.Key, record.Value);
                            await Task.CompletedTask;
                        },
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .Build());
            });
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("key1", "value1");
            Assert.AreEqual(2, numberCall);
            AssertKeyValueOnData( "key1", "value1");
        }
        
        [Test]
        public void KStreamForeachAsyncWithMultipleRetryOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ForeachAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 3)
                                throw new StreamsException("Exception");
                            data.AddOrUpdate(record.Key, record.Value);
                            await Task.CompletedTask;
                        },
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .Build());
            });
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("key1", "value1");
            AssertExtensions.WaitUntil(() => data.Count == 1, TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(100));
            Assert.AreEqual(4, numberCall);
            AssertKeyValueOnData( "key1", "value1");
        }
        
        [Test]
        public void KStreamForeachAsyncRetryAttemptOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ForeachAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            data.AddOrUpdate(record.Key, record.Value);
                            await Task.CompletedTask;
                        },
                        RetryPolicy
                            .NewBuilder()
                            .NumberOfRetry(5)
                            .RetriableException<StreamsException>()
                            .Build());
            });
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("key1", "value1");
            AssertExtensions.WaitUntil(() => data.Count == 1, TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(100)); 
            // null because the default behavior for the retry policy is failed
            Assert.AreEqual(0, data.Count);
        }
        
        [Test]
        public void KStreamForeachAsyncRetryFailed()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ForeachAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            data.AddOrUpdate(record.Key, record.Value);
                            await Task.CompletedTask;
                        },
                        RetryPolicy
                            .NewBuilder()
                            .NumberOfRetry(5)
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.FAIL)
                            .Build());
            });
            var input = driver.CreateInputTopic<string, string>("input");
            Assert.Throws<NoneRetryableException>(() => input.PipeInput("key1", "value1"));
        }
        
        [Test]
        public void KStreamForeachAsyncWithRetryWithoutExceptionConfigurable()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ForeachAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                                throw new StreamsException("Exception");
                            data.AddOrUpdate(record.Key, record.Value);
                            await Task.CompletedTask;
                        });
            }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                (config) => config.InnerExceptionHandler = (e) => ExceptionHandlerResponse.FAIL);
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("key1", "value1");
            Thread.Sleep(500);
            Assert.IsTrue(driver.IsError);
        }
        
        [Test]
        public void KStreamForeachAsyncNotEnoughTimeBufferedException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ForeachAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }

                            data.AddOrUpdate(record.Key, record.Value);
                            await Task.CompletedTask;
                        },
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.BUFFERED)
                            .Build());
            }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                (conf) => conf.MaxPollIntervalMs = 100);
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("key1", "value1");
            AssertExtensions.WaitUntil(() => data.Count == 1, TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(100));
            AssertKeyValueOnData("key1", "value1");
        }

        [Test]
        public void KStreamForeachAsyncNotEnoughTimeSkipException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ForeachAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }

                            data.AddOrUpdate(record.Key, record.Value);
                            await Task.CompletedTask;
                        },
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.SKIP)
                            .Build());
            }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                (conf) => conf.MaxPollIntervalMs = 100);
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("key1", "value1");
            
            AssertExtensions.WaitUntil(() => data.Count == 1, TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(100));
            Assert.AreEqual(0, data.Count);
        }
        
        [Test]
        public void KStreamForeachAsyncNotEnoughTimeFailedException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ForeachAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }

                            data.AddOrUpdate(record.Key, record.Value);
                            await Task.CompletedTask;
                        },
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.FAIL)
                            .Build());
            }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                (conf) => conf.MaxPollIntervalMs = 100);
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("key1", "value1");
            Thread.Sleep(1000);
            Assert.IsTrue(driver.IsError);
        }

        [Test]
        public void KStreamForeachAsyncBufferFull()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .ForeachAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            data.AddOrUpdate(record.Key, record.Value);
                            await Task.CompletedTask;
                        },
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .NumberOfRetry(1)
                            .RetryBackOffMs(1)
                            .MemoryBufferSize(5)
                            .RetryBehavior(EndRetryBehavior.BUFFERED)
                            .Build());
            }, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);
            
            var input = driver.CreateInputTopic<string, string>("input");
            
            for(int i = 0 ; i < 15 ; ++i)
                input.PipeInput($"key{i}", $"value{i}");

            AssertExtensions.WaitUntil(() => data.Count == 15, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(10));
            
            for (int i = 0; i < 15; ++i)
                AssertKeyValueOnData($"key{i}", $"value{i}");
        }

        [Test]
        public void ForeachInnerExceptionHandler()
        {
            using var driver = BuildTopology((builder) =>
                {
                    var stream = builder.Stream<string, string>("input");
                    stream
                        .ForeachAsync(
                            (_, _) => throw new StreamsException("Exception"));
                }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                config => config.InnerExceptionHandler = _ => ExceptionHandlerResponse.CONTINUE);
            
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, string>("output");
            
            for(int i = 0 ; i < 5 ; ++i)
                input.PipeInput("key1", "value1");

            AssertExtensions.WaitUntil(() => data.Count == 5, TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(100));
            Assert.AreEqual(0, data.Count);
            Assert.IsFalse(driver.IsError);
        }
    }
}