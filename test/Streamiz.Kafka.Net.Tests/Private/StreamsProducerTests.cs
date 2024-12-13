using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Private;

public class StreamsProducerTests
{
    [Test]
    public void ThrowExceptionMethodTransactionAtLeastOnce()
    {
        var kafkaSupplier = new SyncKafkaSupplier();
        var streamConfig = new StreamConfig();
        streamConfig.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;

        StreamsProducer streamsProducer = new StreamsProducer(
            streamConfig,
            "thread-1",
            Guid.NewGuid(),
            kafkaSupplier,
            "log-1");

        Assert.Throws<IllegalStateException>(() => streamsProducer.InitTransaction());
        Assert.Throws<IllegalStateException>(() => streamsProducer.CommitTransaction(null, null));
    }
    
    [Test]
    public void ThrowKafkaRetriableExceptionWhenInitTransaction()
    {
        var options = new ProducerSyncExceptionOptions()
        {
            ExceptionsToThrow = new Dictionary<string, Func<Exception>>
            {
                {
                    typeof(IProducer<byte[], byte[]>).GetMethod("InitTransactions")?.Name,
                    () => new KafkaRetriableException(new Error(ErrorCode.Local_TimedOut))
                }
            }
        };
        
        var kafkaSupplier = new ProducerSyncExceptionSupplier(options);
        
        var streamConfig = new StreamConfig();
        streamConfig.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

        StreamsProducer streamsProducer = new StreamsProducer(
            streamConfig,
            "thread-1",
            Guid.NewGuid(),
            kafkaSupplier,
            "log-1");

        Assert.Throws<KafkaRetriableException>(() => streamsProducer.InitTransaction());
    }
    
    [Test]
    public void TestResetProducer()
    {
        var kafkaSupplier = new SyncKafkaSupplier();
        
        var streamConfig = new StreamConfig();
        streamConfig.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

        StreamsProducer streamsProducer = new StreamsProducer(
            streamConfig,
            "thread-1",
            Guid.NewGuid(),
            kafkaSupplier,
            "log-1");

        streamsProducer.InitTransaction();
        
        streamsProducer.Produce("topic", new Message<byte[], byte[]>()
        {
            Key = new byte[]{1},
            Value = new byte[]{1},
        }, (_) => {});
        streamsProducer.Produce("topic", new Message<byte[], byte[]>()
        {
            Key = new byte[]{2},
            Value = new byte[]{2},
        }, (_) => {});
        
        streamsProducer.ResetProducer();
        
        streamsProducer.InitTransaction();
        streamsProducer.Produce("topic", new Message<byte[], byte[]>()
        {
            Key = new byte[]{3},
            Value = new byte[]{3},
        }, (_) => {});

        var producer = (SyncProducer)kafkaSupplier.GetProducer(null);
        Assert.AreEqual(3, producer.GetHistory("topic").ToList().Count);
    }
    
    [Test]
    public void ThrowKafkaRetriableExceptionWhenStartTransactionNoRecoverable()
    {
        var options = new ProducerSyncExceptionOptions()
        {
            ExceptionsToThrow = new Dictionary<string, Func<Exception>>
            {
                {
                    typeof(IProducer<byte[], byte[]>).GetMethod("BeginTransaction")?.Name,
                    () => new KafkaException(new Error(ErrorCode.Local_TimedOut))
                }
            }
        };
        
        var kafkaSupplier = new ProducerSyncExceptionSupplier(options);
        
        var streamConfig = new StreamConfig();
        streamConfig.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

        StreamsProducer streamsProducer = new StreamsProducer(
            streamConfig,
            "thread-1",
            Guid.NewGuid(),
            kafkaSupplier,
            "log-1");

        streamsProducer.InitTransaction();
        Assert.Throws<StreamsException>(() => streamsProducer.Produce("topic", null, (_) => { }));
    }
    
    [Test]
    public void ThrowKafkaRetriableExceptionWhenStartTransactionRecoverable()
    {
        var options = new ProducerSyncExceptionOptions()
        {
            ExceptionsToThrow = new Dictionary<string, Func<Exception>>
            {
                {
                    typeof(IProducer<byte[], byte[]>).GetMethod("BeginTransaction")?.Name,
                    () => new KafkaException(new Error(ErrorCode.ProducerFenced))
                }
            }
        };
        
        var kafkaSupplier = new ProducerSyncExceptionSupplier(options);
        
        var streamConfig = new StreamConfig();
        streamConfig.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

        StreamsProducer streamsProducer = new StreamsProducer(
            streamConfig,
            "thread-1",
            Guid.NewGuid(),
            kafkaSupplier,
            "log-1");

        streamsProducer.InitTransaction();
        Assert.Throws<TaskMigratedException>(() => streamsProducer.Produce("topic", null, (_) => { }));
    }
    
    [Test]
    public void ThrowKafkaRetriableExceptionWhenCommitTransactionNoRecoverable()
    {
        var options = new ProducerSyncExceptionOptions()
        {
            ExceptionsToThrow = new Dictionary<string, Func<Exception>>
            {
                {
                    typeof(IProducer<byte[], byte[]>).GetMethods().First(m => m.Name.Equals("CommitTransaction")).Name,
                    () => new KafkaException(new Error(ErrorCode.Local_TimedOut))
                }
            }
        };
        
        var kafkaSupplier = new ProducerSyncExceptionSupplier(options);
        
        var streamConfig = new StreamConfig();
        streamConfig.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

        StreamsProducer streamsProducer = new StreamsProducer(
            streamConfig,
            "thread-1",
            Guid.NewGuid(),
            kafkaSupplier,
            "log-1");

        streamsProducer.InitTransaction();
        streamsProducer.Produce("topic", null, (_) => { });
        Assert.Throws<StreamsException>(() => streamsProducer.CommitTransaction(null, null));
    }
    
    [Test]
    public void ThrowKafkaRetriableExceptionWhenCommitTransactionRecoverable()
    {
        var options = new ProducerSyncExceptionOptions()
        {
            ExceptionsToThrow = new Dictionary<string, Func<Exception>>
            {
                {
                    typeof(IProducer<byte[], byte[]>).GetMethods().First(m => m.Name.Equals("CommitTransaction")).Name,
                    () => new KafkaException(new Error(ErrorCode.ProducerFenced))
                }
            }
        };
        
        var kafkaSupplier = new ProducerSyncExceptionSupplier(options);
        
        var streamConfig = new StreamConfig();
        streamConfig.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

        StreamsProducer streamsProducer = new StreamsProducer(
            streamConfig,
            "thread-1",
            Guid.NewGuid(),
            kafkaSupplier,
            "log-1");

        streamsProducer.InitTransaction();
        streamsProducer.Produce("topic", null, (_) => { });
        Assert.Throws<TaskMigratedException>(() => streamsProducer.CommitTransaction(null, null));
    }

    [Test]
    public void ThrowKafkaRetriableExceptionWhenAbortTransactionNoRecoverable()
    {
        var options = new ProducerSyncExceptionOptions()
        {
            ExceptionsToThrow = new Dictionary<string, Func<Exception>>
            {
                {
                    typeof(IProducer<byte[], byte[]>).GetMethods().First(m => m.Name.Equals("AbortTransaction")).Name,
                    () => new KafkaException(new Error(ErrorCode.Local_TimedOut))
                }
            }
        };
        
        var kafkaSupplier = new ProducerSyncExceptionSupplier(options);
        
        var streamConfig = new StreamConfig();
        streamConfig.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

        StreamsProducer streamsProducer = new StreamsProducer(
            streamConfig,
            "thread-1",
            Guid.NewGuid(),
            kafkaSupplier,
            "log-1");

        streamsProducer.InitTransaction();
        streamsProducer.Produce("topic", null, (_) => { });
        Assert.Throws<StreamsException>(() => streamsProducer.AbortTransaction());
    }

    [Test]
    public void ThrowKafkaRetriableExceptionWhenAbortTransactionRecoverable()
    {
        var options = new ProducerSyncExceptionOptions()
        {
            ExceptionsToThrow = new Dictionary<string, Func<Exception>>
            {
                {
                    typeof(IProducer<byte[], byte[]>).GetMethods().First(m => m.Name.Equals("AbortTransaction")).Name,
                    () => new KafkaException(new Error(ErrorCode.ProducerFenced))
                }
            }
        };

        var kafkaSupplier = new ProducerSyncExceptionSupplier(options);

        var streamConfig = new StreamConfig();
        streamConfig.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

        StreamsProducer streamsProducer = new StreamsProducer(
            streamConfig,
            "thread-1",
            Guid.NewGuid(),
            kafkaSupplier,
            "log-1");

        streamsProducer.InitTransaction();
        streamsProducer.Produce("topic", null, (_) => { });
        streamsProducer.AbortTransaction();
        Assert.IsFalse(streamsProducer.TransactionInFlight);
    }
}