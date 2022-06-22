using System;
using System.Collections.Generic;
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
    public class KStreamFlatMapAsyncTests : AbstractAsyncTests
    {
        public KStreamFlatMapAsyncTests() 
            : base("test-flatmap-async")
        {
        }

        [Test]
        public void KStreamFlatMapAsyncWithoutRetryOk()
        {
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapAsync(
                        async (record, _) => await Task.FromResult(record.Value.ToCharArray().Select(c => new KeyValuePair<string, char>(record.Key.ToUpper(), c))),
                        null,
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValueList().ToList();
            Assert.AreEqual(6, result.Count);
            
            AssertKeyValue(result[0], "KEY1", 'v');
            AssertKeyValue(result[1], "KEY1", 'a');
            AssertKeyValue(result[2], "KEY1", 'l');
            AssertKeyValue(result[3], "KEY1", 'u');
            AssertKeyValue(result[4], "KEY1", 'e');
            AssertKeyValue(result[5], "KEY1", '1');
        }
        
        [Test]
        public void KStreamFlatMapAsyncWithRetryOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToCharArray().Select(c => new KeyValuePair<string, char>(record.Key.ToUpper(), c)));
                        }, 
                            RetryPolicyBuilder
                                .NewBuilder()
                                .RetriableException<StreamsException>()
                                .Build(), 
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValueList().ToList();
            Assert.AreEqual(2, numberCall);
            
            AssertKeyValue(result[0], "KEY1", 'v');
            AssertKeyValue(result[1], "KEY1", 'a');
            AssertKeyValue(result[2], "KEY1", 'l');
            AssertKeyValue(result[3], "KEY1", 'u');
            AssertKeyValue(result[4], "KEY1", 'e');
            AssertKeyValue(result[5], "KEY1", '1');
        }
        
        [Test]
        public void KStreamFlatMapAsyncWithMultipleRetryOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 3)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToCharArray()
                                .Select(c => new KeyValuePair<string, char>(record.Key.ToUpper(), c)));
                        }, 
                        RetryPolicyBuilder
                            .NewBuilder()
                            .RetriableException<StreamsException>()
                            .Build(),
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValueList().ToList();
            Assert.AreEqual(4, numberCall);
            
            AssertKeyValue(result[0], "KEY1", 'v');
            AssertKeyValue(result[1], "KEY1", 'a');
            AssertKeyValue(result[2], "KEY1", 'l');
            AssertKeyValue(result[3], "KEY1", 'u');
            AssertKeyValue(result[4], "KEY1", 'e');
            AssertKeyValue(result[5], "KEY1", '1');
        }
        
        [Test]
        public void KStreamFlatMapAsyncRetryAttemptOk()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToCharArray().Select(c => new KeyValuePair<string, char>(record.Key.ToUpper(), c)));
                        }, 
                        RetryPolicyBuilder
                            .NewBuilder()
                            .NumberOfRetry(5)
                            .RetriableException<StreamsException>()
                            .Build(),
                        null,
                        new ResponseSerDes<string, char>(new StringSerDes(), new CharSerDes()))
                    .To<StringSerDes, CharSerDes>("output");
            });
            var input = driver.CreateInputTopic<string, string>("input");
            var output = driver.CreateOuputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            var result = output.ReadKeyValue();
            // null because the default behavior for the retry policy is failed
            Assert.IsNull(result);
        }
        
        [Test]
        public void KStreamFlatMapAsyncRetryFailed()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToCharArray().Select(c => new KeyValuePair<string, char>(record.Key.ToUpper(), c)));
                        }, 
                        RetryPolicyBuilder
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
        public void KStreamFlatMapAsyncWithRetryWithoutExceptionConfigurable()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToCharArray().Select(c => new KeyValuePair<string, char>(record.Key.ToUpper(), c)));
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
        public void KStreamFlatMapAsyncNotEnoughTimeBufferedException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }
                            return await Task.FromResult(record.Value.ToCharArray().Select(c => new KeyValuePair<string, char>(record.Key.ToUpper(), c)));
                        }, 
                        RetryPolicyBuilder
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
            var output = driver.CreateOuputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 6);
            
            AssertKeyValue(results[0], "KEY1", 'v');
            AssertKeyValue(results[1], "KEY1", 'a');
            AssertKeyValue(results[2], "KEY1", 'l');
            AssertKeyValue(results[3], "KEY1", 'u');
            AssertKeyValue(results[4], "KEY1", 'e');
            AssertKeyValue(results[5], "KEY1", '1');
        }

        [Test]
        public void KStreamFlatMapAsyncNotEnoughTimeSkipException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }
                            return await Task.FromResult(record.Value.ToCharArray().Select(c => new KeyValuePair<string, char>(record.Key.ToUpper(), c)));
                        }, 
                        RetryPolicyBuilder
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
            var output = driver.CreateOuputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 1, TimeSpan.FromSeconds(1));
            Assert.AreEqual(0, results.Count);
        }
        
        [Test]
        public void KStreamFlatMapAsyncNotEnoughTimeFailedException()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 1)
                            {
                                Thread.Sleep(400);
                                throw new StreamsException("Exception");
                            }
                            return await Task.FromResult(record.Value.ToCharArray().Select(c => new KeyValuePair<string, char>(record.Key.ToUpper(), c)));
                        }, 
                        RetryPolicyBuilder
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
            var output = driver.CreateOuputTopic<string, char, StringSerDes, CharSerDes>("output");
            input.PipeInput("key1", "value1");
            Thread.Sleep(1000);
            Assert.IsTrue(driver.IsError);
        }

        [Test]
        public void KStreamFlatMapAsyncBufferFull()
        {
            int numberCall = 0;
            using var driver = BuildTopology((builder) =>
            {
                var stream = builder.Stream<string, string>("input");
                stream
                    .FlatMapAsync(
                        async (record, _) =>
                        {
                            ++numberCall;
                            if (numberCall <= 10)
                                throw new StreamsException("Exception");
                            return await Task.FromResult(record.Value.ToCharArray().Select(c => new KeyValuePair<string, char>(record.Key.ToUpper(), c)));
                        }, 
                        RetryPolicyBuilder
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
            var output = driver.CreateOuputTopic<string, char, StringSerDes, CharSerDes>("output");
            
            for(int i = 0 ; i < 15 ; ++i)
                input.PipeInput("key1", "value1");
            
            var results = IntegrationTestUtils.WaitUntilMinKeyValueRecordsReceived(output, 15);
            Assert.AreEqual(15 * 6, results.Count);
        }
        
    }
}