using System;
using System.Collections.Generic;
using System.Text;
using log4net;
using log4net.Core;
using log4net.Repository;
using log4net.Repository.Hierarchy;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Mock.Kafka;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class KafkaLoggerAdapterTests
    {
        private class InMemoryLogger : ILogger
        {
            public void Log(Type callerStackBoundaryDeclaringType, Level level, object message, Exception exception)
            {
                Logs.Add(message.ToString());
            }

            public void Log(LoggingEvent logEvent)
            {
                Logs.Add(logEvent.RenderedMessage);
            }

            public bool IsEnabledFor(Level level) => true;

            public List<string> Logs { get; } = new List<string>();

            public ILoggerRepository Repository => new Hierarchy();

            public string Name => "INMEMORY-LOGGER";
        }

        [Test]
        public void TestAdapterLogProducer()
        {
            var mockProducer = new MockProducer(null, "PRODUCER");
            var config = new StreamConfig();
            config.ApplicationId = "test-logger-adapter";
            var logger = new InMemoryLogger();
            var log = new LogImpl(logger);
            var adapter = new KafkaLoggerAdapter(config, log);

            adapter.LogProduce(mockProducer,
                new Confluent.Kafka.LogMessage("error", Confluent.Kafka.SyslogLevel.Critical, "", "error"));

            Assert.AreEqual(1, logger.Logs.Count);
            logger.Logs.Clear();

            adapter.ErrorProduce(mockProducer,
                new Confluent.Kafka.Error(Confluent.Kafka.ErrorCode.BrokerNotAvailable, "error"));

            Assert.AreEqual(1, logger.Logs.Count);
            logger.Logs.Clear();
        }

        [Test]
        public void TestAdapterLogConsumer()
        {
            var mockConsumer = new MockConsumer(null, "group", "CONSUMER");
            var config = new StreamConfig();
            config.ApplicationId = "test-logger-adapter";
            var logger = new InMemoryLogger();
            var log = new LogImpl(logger);
            var adapter = new KafkaLoggerAdapter(config, log);

            adapter.LogConsume(mockConsumer,
                new Confluent.Kafka.LogMessage("error", Confluent.Kafka.SyslogLevel.Critical, "", "error"));

            Assert.AreEqual(1, logger.Logs.Count);
            logger.Logs.Clear();

            adapter.ErrorConsume(mockConsumer,
                new Confluent.Kafka.Error(Confluent.Kafka.ErrorCode.BrokerNotAvailable, "error"));

            Assert.AreEqual(1, logger.Logs.Count);
            logger.Logs.Clear();
        }

        [Test]
        public void TestAdapterLogAdmin()
        {
            var mockAdmin = new MockAdminClient(null, "ADMIN");
            var config = new StreamConfig();
            config.ApplicationId = "test-logger-adapter";
            var logger = new InMemoryLogger();
            var log = new LogImpl(logger);
            var adapter = new KafkaLoggerAdapter(config, log);

            adapter.LogAdmin(mockAdmin,
                new Confluent.Kafka.LogMessage("error", Confluent.Kafka.SyslogLevel.Critical, "", "error"));

            Assert.AreEqual(1, logger.Logs.Count);
            logger.Logs.Clear();

            adapter.ErrorAdmin(mockAdmin,
                new Confluent.Kafka.Error(Confluent.Kafka.ErrorCode.BrokerNotAvailable, "error"));

            Assert.AreEqual(1, logger.Logs.Count);
            logger.Logs.Clear();
        }
    }
}
