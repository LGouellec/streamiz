using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KStreamFlatMapValuesAsyncTests : AbstractAsyncTests
    {
        public KStreamFlatMapValuesAsyncTests() 
            : base("test-flatmap-values-async")
        {
        }

        [Test]
        public void KStreamFlatMapValuesAsyncWithoutRetryOk()
        {
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapValuesAsync(
                        async (record, _) => await Task.FromResult(record.Value.ToCharArray()),
                        null,
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValueList().ToList();
            Assert.AreEqual(6, result.Count);
            
            AssertKeyValue(result[0], "key1", 'v');
            AssertKeyValue(result[1], "key1", 'a');
            AssertKeyValue(result[2], "key1", 'l');
            AssertKeyValue(result[3], "key1", 'u');
            AssertKeyValue(result[4], "key1", 'e');
            AssertKeyValue(result[5], "key1", '1');
        }
        
        [Test]
        public void KStreamFlatMapValuesAsyncWithRetryOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToCharArray());
                        }, 
                            RetryPolicy
                                .NewBuilder()
                                .RetriableException<StreamsException>()
                                .Build(), 
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValueList().ToList();
            Assert.AreEqual(2, numberCall);
            
            AssertKeyValue(result[0], "key1", 'v');
            AssertKeyValue(result[1], "key1", 'a');
            AssertKeyValue(result[2], "key1", 'l');
            AssertKeyValue(result[3], "key1", 'u');
            AssertKeyValue(result[4], "key1", 'e');
            AssertKeyValue(result[5], "key1", '1');
        }
        
        [Test]
        public void KStreamFlatMapValuesAsyncWithMultipleRetryOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 3)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToCharArray());
                        }, 
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .Build(),
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValueList().ToList();
            Assert.AreEqual(4, numberCall);
            
            AssertKeyValue(result[0], "key1", 'v');
            AssertKeyValue(result[1], "key1", 'a');
            AssertKeyValue(result[2], "key1", 'l');
            AssertKeyValue(result[3], "key1", 'u');
            AssertKeyValue(result[4], "key1", 'e');
            AssertKeyValue(result[5], "key1", '1');
        }
        
        [Test]
        public void KStreamFlatMapValuesAsyncRetryAttemptOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToCharArray());
                        }, 
                        RetryPolicy
                            .NewBuilder()
                            .NumberOfRetry(5)
                            .RetriableException<StreamsException>()
                            .Build(),
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValue();
            // null because the default behavior for the retry policy is failed
            Assert.IsNull(result);
        }
        
        [Test]
        public void KStreamFlatMapValuesAsyncRetryFailed()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToCharArray());
                        }, 
                        RetryPolicy
                            .NewBuilder()
                            .NumberOfRetry(5)
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.FAIL)
                            .Build(),
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            Assert.Throws<NoneRetryableException>(() => input.PipeInput("key1", "value1"));
        }
        
        [Test]
        public void KStreamFlatMapValuesAsyncWithRetryWithoutExceptionConfigurable()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToCharArray());
                        },
                        null,
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                (config) => config.InnerExceptionHandler = (e) => ExceptionHandlerResponse.FAIL);
            var input = driver.CreateInputTopic<string, string>("input");
            input.PipeInput("key1", "value1");
            Thread.Sleep(500);
            Assert.IsTrue(driver.IsError);
        }
        
        [Test]
        public void KStreamFlatMapValuesAsyncNotEnoughTimeBufferedException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }
                            return await Task.FromResult(record.Value.ToCharArray());
                        }, 
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.BUFFERED)
                            .Build(),
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                (conf) => conf.MaxPollIntervalMs = 100);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 6);
            
            AssertKeyValue(results[0], "key1", 'v');
            AssertKeyValue(results[1], "key1", 'a');
            AssertKeyValue(results[2], "key1", 'l');
            AssertKeyValue(results[3], "key1", 'u');
            AssertKeyValue(results[4], "key1", 'e');
            AssertKeyValue(results[5], "key1", '1');
        }

        [Test]
        public void KStreamFlatMapValuesAsyncNotEnoughTimeSkipException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }
                            return await Task.FromResult(record.Value.ToCharArray());
                        }, 
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.SKIP)
                            .Build(),
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                (conf) => conf.MaxPollIntervalMs = 100);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 1, TimeSpan.FromSeconds(1));
            Assert.AreEqual(0, results.Count);
        }
        
        [Test]
        public void KStreamFlatMapValuesAsyncNotEnoughTimeFailedException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }
                            return await Task.FromResult(record.Value.ToCharArray());
                        }, 
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .RetryBehavior(EndRetryBehavior.FAIL)
                            .Build(),
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            }, 
                TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY,
                (conf) => conf.MaxPollIntervalMs = 100);
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            Thread.Sleep(1000);
            Assert.IsTrue(driver.IsError);
        }

        [Test]
        public void KStreamFlatMapValuesAsyncBufferFull()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapValuesAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToCharArray());
                        }, 
                        RetryPolicy
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .NumberOfRetry(1)
                            .RetryBackOffMs(1)
                            .MemoryBufferSize(5)
                            .RetryBehavior(EndRetryBehavior.BUFFERED)
                            .Build(),
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            }, TopologyTestDriver.Mode.ASYNC_CLUSTER_IN_MEMORY);
            
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOutputTopic<string, char, StringSerDes, CharSerDes>("output");
            
            for(int i = 0 ; i < 15 ; ++i)
                input.PipeInput("key1", "value1");
            
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 15);
            Assert.AreEqual(15 * 6, results.Count);
        }
        
    }
}