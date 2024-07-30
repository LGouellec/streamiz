using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using Confluent.Kafka;
using Moq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class KafkaLoggerAdapterTests
    {
        private const string Name = "INMEMORY-LOGGER";

        private class InMemoryLogger : ILogger
        {
            [ThreadStatic] private static StringWriter _stringWriter;

            private const string LoglevelPadding = ": ";

            private static readonly string MessagePadding = new string(' ',
                GetLogLevelString(LogLevel.Information).Length + LoglevelPadding.Length);

            private static readonly string NewLineWithMessagePadding = Environment.NewLine + MessagePadding;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception,
                Func<TState, Exception, string> formatter)
            {
                _stringWriter ??= new StringWriter();
                var logEntry = new LogEntry<TState>(logLevel, Name, eventId, state, exception, formatter);
                Write(in logEntry, _stringWriter);
                var sb = _stringWriter.GetStringBuilder();
                string computedAnsiString = sb.ToString();
                Logs.Add(computedAnsiString);
            }

            private void Write<TState>(in LogEntry<TState> logEntry, TextWriter textWriter)
            {
                string message = logEntry.Formatter(logEntry.State, logEntry.Exception);
                if (logEntry.Exception == null && message == null)
                {
                    return;
                }

                LogLevel logLevel = logEntry.LogLevel;
                string logLevelString = GetLogLevelString(logLevel);

                string timestampFormat = "yyyy'-'MM'-'dd'T'HH':'mm':'ss";
                DateTimeOffset dateTimeOffset = DateTimeOffset.UtcNow;
                string timestamp = dateTimeOffset.ToString(timestampFormat);


                textWriter.Write(timestamp);
                if (logLevelString != null)
                {
                    textWriter.Write(logLevelString);
                }

                CreateDefaultLogMessage(textWriter, logEntry, message);
            }

            private void CreateDefaultLogMessage<TState>(TextWriter textWriter, in LogEntry<TState> logEntry,
                string message)
            {
                int eventId = logEntry.EventId.Id;
                Exception exception = logEntry.Exception;

                // Example:
                // info: ConsoleApp.Program[10]
                //       Request received

                // category and event id
                textWriter.Write(LoglevelPadding + logEntry.Category + '[' + eventId + "]");
                textWriter.Write(Environment.NewLine);

                WriteMessage(textWriter, message);

                // Example:
                // System.InvalidOperationException
                //    at Namespace.Class.Function() in File:line X
                if (exception != null)
                {
                    // exception message
                    WriteMessage(textWriter, exception.ToString());
                }

                textWriter.Write(Environment.NewLine);
            }

            private void WriteMessage(TextWriter textWriter, string message)
            {
                if (string.IsNullOrEmpty(message)) return;
                textWriter.Write(MessagePadding);
                string newMessage = message.Replace(Environment.NewLine, NewLineWithMessagePadding);
                textWriter.Write(newMessage);
                textWriter.Write(Environment.NewLine);
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return true;
            }

            public IDisposable BeginScope<TState>(TState state)
            {
                return null;
            }

            private static string GetLogLevelString(LogLevel logLevel)
            {
                return logLevel switch
                {
                    LogLevel.Trace => "trce",
                    LogLevel.Debug => "dbug",
                    LogLevel.Information => "info",
                    LogLevel.Warning => "warn",
                    LogLevel.Error => "fail",
                    LogLevel.Critical => "crit",
                    _ => throw new ArgumentOutOfRangeException(nameof(logLevel))
                };
            }

            public List<string> Logs { get; } = new List<string>();
        }

        [Test]
        public void LogAndErrorWithThreadName()
        {
            Thread.CurrentThread.Name = "test-adapter";
            
            var mockConsumer = new MockConsumer(null, "group", "CONSUMER");
            var mockProducer = new MockProducer(null, "PRODUCER");
            var mockAdmin = new MockAdminClient(null, "ADMIN");
            
            var config = new StreamConfig();
            config.ApplicationId = "test-logger-adapter";
            var logger = new InMemoryLogger();
            var adapter = new KafkaLoggerAdapter(config, logger);

            adapter.LogConsume(mockConsumer,
                new Confluent.Kafka.LogMessage("error", Confluent.Kafka.SyslogLevel.Critical, "", "error"));
            adapter.ErrorConsume(mockConsumer, new Error(ErrorCode.ConcurrentTransactions));

            adapter.LogProduce(mockProducer,
                new Confluent.Kafka.LogMessage("error", Confluent.Kafka.SyslogLevel.Critical, "", "error"));
            adapter.ErrorProduce(mockProducer, new Error(ErrorCode.RecordListTooLarge));
            
            adapter.LogAdmin(mockAdmin,
                new Confluent.Kafka.LogMessage("error", Confluent.Kafka.SyslogLevel.Critical, "", "error"));
            adapter.ErrorAdmin(mockAdmin,
                new Confluent.Kafka.Error(Confluent.Kafka.ErrorCode.ClusterAuthorizationFailed));
        }
        
        [Test]
        public void TestAdapterLogProducer()
        {
            var mockProducer = new MockProducer(null, "PRODUCER");
            var config = new StreamConfig();
            config.ApplicationId = "test-logger-adapter";
            var logger = new InMemoryLogger();
            var adapter = new KafkaLoggerAdapter(config, logger);

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
            var adapter = new KafkaLoggerAdapter(config, logger);

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
            var adapter = new KafkaLoggerAdapter(config, logger);

            adapter.LogAdmin(mockAdmin,
                new Confluent.Kafka.LogMessage("error", Confluent.Kafka.SyslogLevel.Critical, "", "error"));

            Assert.AreEqual(1, logger.Logs.Count);
            logger.Logs.Clear();

            adapter.ErrorAdmin(mockAdmin,
                new Confluent.Kafka.Error(Confluent.Kafka.ErrorCode.BrokerNotAvailable, "error"));

            Assert.AreEqual(1, logger.Logs.Count);
            logger.Logs.Clear();
        }
        
       [Test]
       public void TestAdapterGetNameNPE()
        {
                var config = new StreamConfig();
                config.ApplicationId = "test-logger-adapter";
                var logger = new InMemoryLogger();
                var adapter = new KafkaLoggerAdapter(config, logger);

                var client = new Mock<IConsumer<byte[], byte[]>>();
                client
                    .Setup(c => c.Name)
                    .Throws<NullReferenceException>();

               int v = 1233;
                Type handleType = typeof(object);
                object handle = ReflectionHelperExtensionMethods.GetInstance(
                    "Confluent.Kafka.Impl.SafeKafkaHandle", ref handleType);
                handle.SetPrivateFieldsValue(handleType, "handle", new IntPtr(v));

                Handle h = new Handle();
                h.SetPrivatePropertyValue("LibrdkafkaHandle", handle);

                client.Setup(c => c.Handle)
                    .Returns(() => h);

                adapter.LogConsume(client.Object,
                    new Confluent.Kafka.LogMessage("error", Confluent.Kafka.SyslogLevel.Critical, "", "error"));

                handle.SetPrivateFieldsValue(handleType, "handle", IntPtr.Zero);
                handle = null;
        }
        
        [Test]
        public void TestAdapterNullHandle()
        {
            var config = new StreamConfig();
            config.ApplicationId = "test-logger-adapter";
            var logger = new InMemoryLogger();
            var adapter = new KafkaLoggerAdapter(config, logger);
            
            var client = new Mock<IConsumer<byte[], byte[]>>();
            client
                .Setup(c => c.Name)
                .Throws<NullReferenceException>();
            
            client.Setup(c => c.Handle)
                .Returns(() => new Handle());
            
            adapter.LogConsume(client.Object,
                new Confluent.Kafka.LogMessage("error", Confluent.Kafka.SyslogLevel.Critical, "", "error"));
        }
        
        [Test]
        public void TestAdapterHandleInvalid()
        {
            var config = new StreamConfig();
            config.ApplicationId = "test-logger-adapter";
            var logger = new InMemoryLogger();
            var adapter = new KafkaLoggerAdapter(config, logger);
            
            var client = new Mock<IConsumer<byte[], byte[]>>();
            client
                .Setup(c => c.Name)
                .Throws<NullReferenceException>();
            
            Type handleType = typeof(object);
            object handle = ReflectionHelperExtensionMethods.GetInstance(
                "Confluent.Kafka.Impl.SafeKafkaHandle", ref handleType);
            Handle h = new Handle();
            h.SetPrivatePropertyValue("LibrdkafkaHandle", handle);
            
            client.Setup(c => c.Handle)
                .Returns(() => h);
            
            adapter.LogConsume(client.Object,
                new Confluent.Kafka.LogMessage("error", Confluent.Kafka.SyslogLevel.Critical, "", "error"));

            handle = null;
        }
    }
}