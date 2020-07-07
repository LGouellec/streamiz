using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class StreamConfigTests
    {
        public void StreamNoApplicationId()
        {
            var stream = new StreamConfig();
            stream.AddConfig("sasl.password", "coucou");
            Assert.Throws<StreamConfigException>(() => stream.ToConsumerConfig());
        }

        public void StreamConfigurationIsNotCorrect()
        {
            var config = new StreamConfig();
            var builder = new StreamBuilder();
            Assert.Throws<StreamConfigException>(() => new KafkaStream(builder.Build(), config));
        }

        [Test]
        public void StreamAddCorrectConfig()
        {
            var stream = new StreamConfig();
            stream.ApplicationId = "unittest";
            stream.AddConfig("sasl.password", "coucou");

            var adminConfig = stream.ToAdminConfig("admin");
            var consumerConfig = stream.ToConsumerConfig();
            var producerConfig = stream.ToProducerConfig();
            var globalConfig = stream.ToGlobalConsumerConfig("global");

            Assert.AreEqual("coucou", adminConfig.SaslPassword);
            Assert.AreEqual("coucou", consumerConfig.SaslPassword);
            Assert.AreEqual("coucou", producerConfig.SaslPassword);
            Assert.AreEqual("coucou", globalConfig.SaslPassword);
        }

        [Test]
        public void StreamIncorrectNumberThreadProperty()
        {
            var stream = new StreamConfig();
            stream.ApplicationId = "test";
            Assert.Throws<StreamConfigException>(() => stream.NumStreamThreads = -1);
        }

        [Test]
        public void StreamCompleteConfigProperty()
        {
            var stream = new StreamConfig();
            stream.ApplicationId = "test";

            stream.Guarantee = ProcessingGuarantee.EXACTLY_ONCE;

            Assert.AreEqual(ProcessingGuarantee.EXACTLY_ONCE, stream.Guarantee);
            Assert.AreEqual(Confluent.Kafka.IsolationLevel.ReadCommitted, stream.IsolationLevel);
            Assert.AreEqual(true, stream.EnableIdempotence);
            Assert.AreEqual(5, stream.MaxInFlight);
            Assert.AreEqual(StreamConfig.EOS_DEFAULT_COMMIT_INTERVAL_MS, stream.CommitIntervalMs);

            stream.DefaultKeySerDes = new StringSerDes();
            stream.DefaultValueSerDes = new StringSerDes();
            stream.DefaultTimestampExtractor = new FailOnInvalidTimestamp();
            stream.TransactionTimeout = TimeSpan.FromSeconds(10);
            stream.CommitIntervalMs = 12;
            stream.PollMs = 150;

            Assert.AreEqual("test", stream.ApplicationId);
            Assert.AreEqual(ProcessingGuarantee.EXACTLY_ONCE, stream.Guarantee);
            Assert.IsInstanceOf<StringSerDes>(stream.DefaultKeySerDes);
            Assert.IsInstanceOf<StringSerDes>(stream.DefaultValueSerDes);
            Assert.IsInstanceOf<FailOnInvalidTimestamp>(stream.DefaultTimestampExtractor);
            Assert.AreEqual(TimeSpan.FromSeconds(10), stream.TransactionTimeout);
            Assert.AreEqual(12, stream.CommitIntervalMs);
            Assert.AreEqual(150, stream.PollMs);
        }

        [Test]
        public void StreamCompleteConfigAllProperty()
        {
            var stream = new StreamConfig();
            stream.ApplicationId = "test";
            stream.Acks = Confluent.Kafka.Acks.All;
            stream.ApiVersionFallbackMs = 1;
            stream.ApiVersionRequest = false;
            stream.ApiVersionRequestTimeoutMs = 100;
            stream.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Latest;
            stream.BatchNumMessages = 42;
            stream.BootstrapServers = "127.0.0.1:9092";
            stream.BrokerAddressFamily = Confluent.Kafka.BrokerAddressFamily.V4;
            stream.BrokerAddressTtl = 100;
            stream.BrokerVersionFallback = "0.12.0";
            stream.CheckCrcs = true;
            stream.ClientId = "test-client";
            stream.ClientRack = "1";
            stream.CommitIntervalMs = 300;
            stream.CompressionLevel = 2;
            stream.CompressionType = Confluent.Kafka.CompressionType.Snappy;
            stream.ConsumeResultFields = "all";
            stream.CoordinatorQueryIntervalMs = 300;
            stream.Debug = "all";
            stream.DeliveryReportFields = "key";
            stream.EnableAutoOffsetStore = false;
            stream.EnableBackgroundPoll = false;
            stream.EnableDeliveryReports = false;
            stream.EnableGaplessGuarantee = false;
            stream.EnableIdempotence = true;
            stream.EnablePartitionEof = true;
            stream.EnableSaslOauthbearerUnsecureJwt = true;
            stream.EnableSslCertificateVerification = false;
            stream.FetchErrorBackoffMs = 10;
            stream.FetchMaxBytes = 10;
            stream.FetchMinBytes = 10;
            stream.FetchWaitMaxMs = 10;
            stream.GroupProtocolType = "?";
            stream.HeartbeatIntervalMs = 4000;
            stream.InternalTerminationSignal = 1;
            stream.IsolationLevel = Confluent.Kafka.IsolationLevel.ReadCommitted;
            stream.LingerMs = 12;
            stream.LogConnectionClose = false;
            stream.LogQueue = true;
            stream.LogThreadName = false;
            stream.MaxInFlight = 12;
            stream.MaxPartitionFetchBytes = 500;
            stream.MaxPollIntervalMs = 400;
            stream.MessageCopyMaxBytes = 40;
            stream.MessageMaxBytes = 500;
            stream.MessageSendMaxRetries = 4;
            stream.MessageTimeoutMs = 600;
            stream.MetadataMaxAgeMs = 6;
            stream.MetadataRequestTimeoutMs = 83;
            stream.Partitioner = Confluent.Kafka.Partitioner.Murmur2Random;
            stream.PluginLibraryPaths = "D:";
            stream.QueueBufferingBackpressureThreshold = 10;
            stream.QueueBufferingMaxKbytes = 400;
            stream.QueueBufferingMaxMessages = 5;
            stream.QueuedMaxMessagesKbytes = 800;
            stream.QueuedMinMessages = 1;
            stream.ReceiveMessageMaxBytes = 1000;
            stream.ReconnectBackoffMaxMs = 9000;
            stream.ReconnectBackoffMs = 8000;
            stream.RequestTimeoutMs = 16600;
            stream.RetryBackoffMs = 600;
            stream.SaslKerberosKeytab = "test";
            stream.SaslKerberosKinitCmd = "test";
            stream.SaslKerberosMinTimeBeforeRelogin = 600;
            stream.SaslKerberosPrincipal = "Princiapl";
            stream.SaslKerberosServiceName = "kerberos";
            stream.SaslMechanism = Confluent.Kafka.SaslMechanism.ScramSha512;
            stream.SaslOauthbearerConfig = "ouath";
            stream.SaslPassword = "test";
            stream.SaslUsername = "admin";
            stream.SecurityProtocol = Confluent.Kafka.SecurityProtocol.SaslPlaintext;
            stream.SessionTimeoutMs = 1000;
            stream.SocketKeepaliveEnable = true;
            stream.SocketMaxFails = 2;
            stream.SocketNagleDisable = true;
            stream.SocketReceiveBufferBytes = 50000;
            stream.SocketSendBufferBytes = 50000;
            stream.SocketTimeoutMs = 6000;
            stream.SslCaLocation = "D:";
            stream.SslCertificateLocation = "D:";
            stream.SslCertificatePem = "D:";
            stream.SslCipherSuites = "ciphers";
            stream.SslCrlLocation = "D:";
            stream.SslCurvesList = "";
            stream.SslEndpointIdentificationAlgorithm = Confluent.Kafka.SslEndpointIdentificationAlgorithm.Https;
            stream.SslKeyLocation = "C:";
            stream.SslKeyPassword = "test";
            stream.SslKeyPem = "pem";
            stream.SslKeystoreLocation = "J:";
            stream.SslKeystorePassword = "password";
            stream.SslSigalgsList = "oepn";
            stream.StatisticsIntervalMs = 14;
            stream.TopicBlacklist = "*";
            stream.TopicMetadataRefreshFastIntervalMs = 500;
            stream.TopicMetadataRefreshIntervalMs = 200;
            stream.TopicMetadataRefreshSparse = false;
            stream.TransactionalId = "transac";
            stream.TransactionTimeout = TimeSpan.FromSeconds(1);
            stream.TransactionTimeoutMs = 400;

            var producerConfig = stream.ToProducerConfig();
            var consumerConfig = stream.ToConsumerConfig();
            var globalConfig = stream.ToGlobalConsumerConfig("global");
            var adminConfig = stream.ToAdminConfig("admin");

            #region ProducerConfig
            Assert.AreEqual(Confluent.Kafka.Acks.All, producerConfig.Acks);
            Assert.AreEqual(1, producerConfig.ApiVersionFallbackMs);
            Assert.AreEqual(false, producerConfig.ApiVersionRequest);
            Assert.AreEqual(100, producerConfig.ApiVersionRequestTimeoutMs);
            Assert.AreEqual(42, producerConfig.BatchNumMessages);
            Assert.AreEqual("127.0.0.1:9092", producerConfig.BootstrapServers);
            Assert.AreEqual(Confluent.Kafka.BrokerAddressFamily.V4, producerConfig.BrokerAddressFamily);
            Assert.AreEqual(100, producerConfig.BrokerAddressTtl);
            Assert.AreEqual("0.12.0", producerConfig.BrokerVersionFallback);
            Assert.AreEqual("test-client", producerConfig.ClientId);
            Assert.AreEqual("1", producerConfig.ClientRack);
            Assert.AreEqual(2, producerConfig.CompressionLevel);
            Assert.AreEqual(Confluent.Kafka.CompressionType.Snappy, producerConfig.CompressionType);
            Assert.AreEqual("all", producerConfig.Debug);
            Assert.AreEqual("key", producerConfig.DeliveryReportFields);
            Assert.AreEqual(false, producerConfig.EnableBackgroundPoll);
            Assert.AreEqual(false, producerConfig.EnableDeliveryReports);
            Assert.AreEqual(false, producerConfig.EnableGaplessGuarantee);
            Assert.AreEqual(true, producerConfig.EnableIdempotence);
            Assert.AreEqual(true, producerConfig.EnableSaslOauthbearerUnsecureJwt);
            Assert.AreEqual(false, producerConfig.EnableSslCertificateVerification);
            Assert.AreEqual(1, producerConfig.InternalTerminationSignal);
            Assert.AreEqual(12, producerConfig.LingerMs);
            Assert.AreEqual(false, producerConfig.LogConnectionClose);
            Assert.AreEqual(true, producerConfig.LogQueue);
            Assert.AreEqual(false, producerConfig.LogThreadName);
            Assert.AreEqual(12, producerConfig.MaxInFlight);
            Assert.AreEqual(40, producerConfig.MessageCopyMaxBytes);
            Assert.AreEqual(500, producerConfig.MessageMaxBytes);
            Assert.AreEqual(4, producerConfig.MessageSendMaxRetries);
            Assert.AreEqual(600, producerConfig.MessageTimeoutMs);
            Assert.AreEqual(6, producerConfig.MetadataMaxAgeMs);
            Assert.AreEqual(83, producerConfig.MetadataRequestTimeoutMs);
            Assert.AreEqual(Confluent.Kafka.Partitioner.Murmur2Random, producerConfig.Partitioner);
            Assert.AreEqual("D:", producerConfig.PluginLibraryPaths);
            Assert.AreEqual(10, producerConfig.QueueBufferingBackpressureThreshold);
            Assert.AreEqual(400, producerConfig.QueueBufferingMaxKbytes);
            Assert.AreEqual(5, producerConfig.QueueBufferingMaxMessages);
            Assert.AreEqual(1000, producerConfig.ReceiveMessageMaxBytes);
            Assert.AreEqual(9000, producerConfig.ReconnectBackoffMaxMs);
            Assert.AreEqual(8000, producerConfig.ReconnectBackoffMs);
            Assert.AreEqual(16600, producerConfig.RequestTimeoutMs);
            Assert.AreEqual(600, producerConfig.RetryBackoffMs);
            Assert.AreEqual("test", producerConfig.SaslKerberosKeytab);
            Assert.AreEqual("test", producerConfig.SaslKerberosKinitCmd);
            Assert.AreEqual(600, producerConfig.SaslKerberosMinTimeBeforeRelogin);
            Assert.AreEqual("Princiapl", producerConfig.SaslKerberosPrincipal);
            Assert.AreEqual("kerberos", producerConfig.SaslKerberosServiceName);
            Assert.AreEqual(Confluent.Kafka.SaslMechanism.ScramSha512, producerConfig.SaslMechanism);
            Assert.AreEqual("ouath", producerConfig.SaslOauthbearerConfig);
            Assert.AreEqual("test", producerConfig.SaslPassword);
            Assert.AreEqual("admin", producerConfig.SaslUsername);
            Assert.AreEqual(Confluent.Kafka.SecurityProtocol.SaslPlaintext, producerConfig.SecurityProtocol);
            Assert.AreEqual(true, producerConfig.SocketKeepaliveEnable);
            Assert.AreEqual(2, producerConfig.SocketMaxFails);
            Assert.AreEqual(true, producerConfig.SocketNagleDisable);
            Assert.AreEqual(50000, producerConfig.SocketReceiveBufferBytes);
            Assert.AreEqual(50000, producerConfig.SocketSendBufferBytes);
            Assert.AreEqual(6000, producerConfig.SocketTimeoutMs);
            Assert.AreEqual("D:", producerConfig.SslCaLocation);
            Assert.AreEqual("D:", producerConfig.SslCertificateLocation);
            Assert.AreEqual("D:", producerConfig.SslCertificatePem);
            Assert.AreEqual("ciphers", producerConfig.SslCipherSuites);
            Assert.AreEqual("D:", producerConfig.SslCrlLocation);
            Assert.AreEqual("", producerConfig.SslCurvesList);
            Assert.AreEqual(Confluent.Kafka.SslEndpointIdentificationAlgorithm.Https, producerConfig.SslEndpointIdentificationAlgorithm);
            Assert.AreEqual("C:", producerConfig.SslKeyLocation);
            Assert.AreEqual("test", producerConfig.SslKeyPassword);
            Assert.AreEqual("pem", producerConfig.SslKeyPem);
            Assert.AreEqual("J:", producerConfig.SslKeystoreLocation);
            Assert.AreEqual("password", producerConfig.SslKeystorePassword);
            Assert.AreEqual("oepn", producerConfig.SslSigalgsList);
            Assert.AreEqual(14, producerConfig.StatisticsIntervalMs);
            Assert.AreEqual("*", producerConfig.TopicBlacklist);
            Assert.AreEqual(500, producerConfig.TopicMetadataRefreshFastIntervalMs);
            Assert.AreEqual(200, producerConfig.TopicMetadataRefreshIntervalMs);
            Assert.AreEqual(false, producerConfig.TopicMetadataRefreshSparse);
            Assert.AreEqual("transac", producerConfig.TransactionalId);
            Assert.AreEqual(400, producerConfig.TransactionTimeoutMs);

            #endregion

            #region ConsumerConfig
            Assert.AreEqual(Confluent.Kafka.Acks.All, consumerConfig.Acks);
            Assert.AreEqual(1, consumerConfig.ApiVersionFallbackMs);
            Assert.AreEqual(false, consumerConfig.ApiVersionRequest);
            Assert.AreEqual(100, consumerConfig.ApiVersionRequestTimeoutMs);
            Assert.AreEqual(Confluent.Kafka.AutoOffsetReset.Latest, consumerConfig.AutoOffsetReset);
            Assert.AreEqual("127.0.0.1:9092", consumerConfig.BootstrapServers);
            Assert.AreEqual(Confluent.Kafka.BrokerAddressFamily.V4, consumerConfig.BrokerAddressFamily);
            Assert.AreEqual(100, consumerConfig.BrokerAddressTtl);
            Assert.AreEqual("0.12.0", consumerConfig.BrokerVersionFallback);
            Assert.AreEqual(true, consumerConfig.CheckCrcs);
            Assert.AreEqual("test-client", consumerConfig.ClientId);
            Assert.AreEqual("1", consumerConfig.ClientRack);
            Assert.AreEqual(300, consumerConfig.CoordinatorQueryIntervalMs);
            Assert.AreEqual("all", consumerConfig.Debug);
            Assert.AreEqual(false, consumerConfig.EnableAutoOffsetStore);
            Assert.AreEqual(true, consumerConfig.EnablePartitionEof);
            Assert.AreEqual(true, consumerConfig.EnableSaslOauthbearerUnsecureJwt);
            Assert.AreEqual(false, consumerConfig.EnableSslCertificateVerification);
            Assert.AreEqual(10, consumerConfig.FetchErrorBackoffMs);
            Assert.AreEqual(10, consumerConfig.FetchMaxBytes);
            Assert.AreEqual(10, consumerConfig.FetchMinBytes);
            Assert.AreEqual(10, consumerConfig.FetchWaitMaxMs);
            Assert.AreEqual("?", consumerConfig.GroupProtocolType);
            Assert.AreEqual(4000, consumerConfig.HeartbeatIntervalMs);
            Assert.AreEqual(1, consumerConfig.InternalTerminationSignal);
            Assert.AreEqual(Confluent.Kafka.IsolationLevel.ReadCommitted, consumerConfig.IsolationLevel);
            Assert.AreEqual(false, consumerConfig.LogConnectionClose);
            Assert.AreEqual(true, consumerConfig.LogQueue);
            Assert.AreEqual(false, consumerConfig.LogThreadName);
            Assert.AreEqual(12, consumerConfig.MaxInFlight);
            Assert.AreEqual(500, consumerConfig.MaxPartitionFetchBytes);
            Assert.AreEqual(400, consumerConfig.MaxPollIntervalMs);
            Assert.AreEqual(40, consumerConfig.MessageCopyMaxBytes);
            Assert.AreEqual(500, consumerConfig.MessageMaxBytes);
            Assert.AreEqual(6, consumerConfig.MetadataMaxAgeMs);
            Assert.AreEqual(83, consumerConfig.MetadataRequestTimeoutMs);
            Assert.AreEqual(Confluent.Kafka.PartitionAssignmentStrategy.Range, consumerConfig.PartitionAssignmentStrategy);
            Assert.AreEqual("D:", consumerConfig.PluginLibraryPaths);
            Assert.AreEqual(800, consumerConfig.QueuedMaxMessagesKbytes);
            Assert.AreEqual(1, consumerConfig.QueuedMinMessages);
            Assert.AreEqual(1000, consumerConfig.ReceiveMessageMaxBytes);
            Assert.AreEqual(9000, consumerConfig.ReconnectBackoffMaxMs);
            Assert.AreEqual(8000, consumerConfig.ReconnectBackoffMs);
            Assert.AreEqual("test", consumerConfig.SaslKerberosKeytab);
            Assert.AreEqual("test", consumerConfig.SaslKerberosKinitCmd);
            Assert.AreEqual(600, consumerConfig.SaslKerberosMinTimeBeforeRelogin);
            Assert.AreEqual("Princiapl", consumerConfig.SaslKerberosPrincipal);
            Assert.AreEqual("kerberos", consumerConfig.SaslKerberosServiceName);
            Assert.AreEqual(Confluent.Kafka.SaslMechanism.ScramSha512, consumerConfig.SaslMechanism);
            Assert.AreEqual("ouath", consumerConfig.SaslOauthbearerConfig);
            Assert.AreEqual("test", consumerConfig.SaslPassword);
            Assert.AreEqual("admin", consumerConfig.SaslUsername);
            Assert.AreEqual(Confluent.Kafka.SecurityProtocol.SaslPlaintext, consumerConfig.SecurityProtocol);
            Assert.AreEqual(1000, consumerConfig.SessionTimeoutMs);
            Assert.AreEqual(true, consumerConfig.SocketKeepaliveEnable);
            Assert.AreEqual(2, consumerConfig.SocketMaxFails);
            Assert.AreEqual(true, consumerConfig.SocketNagleDisable);
            Assert.AreEqual(50000, consumerConfig.SocketReceiveBufferBytes);
            Assert.AreEqual(50000, consumerConfig.SocketSendBufferBytes);
            Assert.AreEqual(6000, consumerConfig.SocketTimeoutMs);
            Assert.AreEqual("D:", consumerConfig.SslCaLocation);
            Assert.AreEqual("D:", consumerConfig.SslCertificateLocation);
            Assert.AreEqual("D:", consumerConfig.SslCertificatePem);
            Assert.AreEqual("ciphers", consumerConfig.SslCipherSuites);
            Assert.AreEqual("D:", consumerConfig.SslCrlLocation);
            Assert.AreEqual("", consumerConfig.SslCurvesList);
            Assert.AreEqual(Confluent.Kafka.SslEndpointIdentificationAlgorithm.Https, consumerConfig.SslEndpointIdentificationAlgorithm);
            Assert.AreEqual("C:", consumerConfig.SslKeyLocation);
            Assert.AreEqual("test", consumerConfig.SslKeyPassword);
            Assert.AreEqual("pem", consumerConfig.SslKeyPem);
            Assert.AreEqual("J:", consumerConfig.SslKeystoreLocation);
            Assert.AreEqual("password", consumerConfig.SslKeystorePassword);
            Assert.AreEqual("oepn", consumerConfig.SslSigalgsList);
            Assert.AreEqual(14, consumerConfig.StatisticsIntervalMs);
            Assert.AreEqual("*", consumerConfig.TopicBlacklist);
            Assert.AreEqual(500, consumerConfig.TopicMetadataRefreshFastIntervalMs);
            Assert.AreEqual(200, consumerConfig.TopicMetadataRefreshIntervalMs);
            Assert.AreEqual(false, consumerConfig.TopicMetadataRefreshSparse);
            #endregion

            #region GlobalConfig
            Assert.AreEqual(Confluent.Kafka.Acks.All, globalConfig.Acks);
            Assert.AreEqual(1, globalConfig.ApiVersionFallbackMs);
            Assert.AreEqual(false, globalConfig.ApiVersionRequest);
            Assert.AreEqual(100, globalConfig.ApiVersionRequestTimeoutMs);
            Assert.AreEqual(Confluent.Kafka.AutoOffsetReset.Earliest, globalConfig.AutoOffsetReset);
            Assert.AreEqual("127.0.0.1:9092", globalConfig.BootstrapServers);
            Assert.AreEqual(Confluent.Kafka.BrokerAddressFamily.V4, globalConfig.BrokerAddressFamily);
            Assert.AreEqual(100, globalConfig.BrokerAddressTtl);
            Assert.AreEqual("0.12.0", globalConfig.BrokerVersionFallback);
            Assert.AreEqual(true, globalConfig.CheckCrcs);
            Assert.AreEqual("global", globalConfig.ClientId);
            Assert.AreEqual("1", globalConfig.ClientRack);
            Assert.AreEqual(300, globalConfig.CoordinatorQueryIntervalMs);
            Assert.AreEqual("all", globalConfig.Debug);
            Assert.AreEqual(false, globalConfig.EnableAutoOffsetStore);
            Assert.AreEqual(true, globalConfig.EnablePartitionEof);
            Assert.AreEqual(true, globalConfig.EnableSaslOauthbearerUnsecureJwt);
            Assert.AreEqual(false, globalConfig.EnableSslCertificateVerification);
            Assert.AreEqual(10, globalConfig.FetchErrorBackoffMs);
            Assert.AreEqual(10, globalConfig.FetchMaxBytes);
            Assert.AreEqual(10, globalConfig.FetchMinBytes);
            Assert.AreEqual(10, globalConfig.FetchWaitMaxMs);
            Assert.AreEqual("?", globalConfig.GroupProtocolType);
            Assert.AreEqual(4000, globalConfig.HeartbeatIntervalMs);
            Assert.AreEqual(1, globalConfig.InternalTerminationSignal);
            Assert.AreEqual(Confluent.Kafka.IsolationLevel.ReadCommitted, globalConfig.IsolationLevel);
            Assert.AreEqual(false, globalConfig.LogConnectionClose);
            Assert.AreEqual(true, globalConfig.LogQueue);
            Assert.AreEqual(false, globalConfig.LogThreadName);
            Assert.AreEqual(12, globalConfig.MaxInFlight);
            Assert.AreEqual(500, globalConfig.MaxPartitionFetchBytes);
            Assert.AreEqual(400, globalConfig.MaxPollIntervalMs);
            Assert.AreEqual(40, globalConfig.MessageCopyMaxBytes);
            Assert.AreEqual(500, globalConfig.MessageMaxBytes);
            Assert.AreEqual(6, globalConfig.MetadataMaxAgeMs);
            Assert.AreEqual(83, globalConfig.MetadataRequestTimeoutMs);
            Assert.AreEqual(Confluent.Kafka.PartitionAssignmentStrategy.Range, globalConfig.PartitionAssignmentStrategy);
            Assert.AreEqual("D:", globalConfig.PluginLibraryPaths);
            Assert.AreEqual(800, globalConfig.QueuedMaxMessagesKbytes);
            Assert.AreEqual(1, globalConfig.QueuedMinMessages);
            Assert.AreEqual(1000, globalConfig.ReceiveMessageMaxBytes);
            Assert.AreEqual(9000, globalConfig.ReconnectBackoffMaxMs);
            Assert.AreEqual(8000, globalConfig.ReconnectBackoffMs);
            Assert.AreEqual("test", globalConfig.SaslKerberosKeytab);
            Assert.AreEqual("test", globalConfig.SaslKerberosKinitCmd);
            Assert.AreEqual(600, globalConfig.SaslKerberosMinTimeBeforeRelogin);
            Assert.AreEqual("Princiapl", globalConfig.SaslKerberosPrincipal);
            Assert.AreEqual("kerberos", globalConfig.SaslKerberosServiceName);
            Assert.AreEqual(Confluent.Kafka.SaslMechanism.ScramSha512, globalConfig.SaslMechanism);
            Assert.AreEqual("ouath", globalConfig.SaslOauthbearerConfig);
            Assert.AreEqual("test", globalConfig.SaslPassword);
            Assert.AreEqual("admin", globalConfig.SaslUsername);
            Assert.AreEqual(Confluent.Kafka.SecurityProtocol.SaslPlaintext, globalConfig.SecurityProtocol);
            Assert.AreEqual(1000, globalConfig.SessionTimeoutMs);
            Assert.AreEqual(true, globalConfig.SocketKeepaliveEnable);
            Assert.AreEqual(2, globalConfig.SocketMaxFails);
            Assert.AreEqual(true, globalConfig.SocketNagleDisable);
            Assert.AreEqual(50000, globalConfig.SocketReceiveBufferBytes);
            Assert.AreEqual(50000, globalConfig.SocketSendBufferBytes);
            Assert.AreEqual(6000, globalConfig.SocketTimeoutMs);
            Assert.AreEqual("D:", globalConfig.SslCaLocation);
            Assert.AreEqual("D:", globalConfig.SslCertificateLocation);
            Assert.AreEqual("D:", globalConfig.SslCertificatePem);
            Assert.AreEqual("ciphers", globalConfig.SslCipherSuites);
            Assert.AreEqual("D:", globalConfig.SslCrlLocation);
            Assert.AreEqual("", globalConfig.SslCurvesList);
            Assert.AreEqual(Confluent.Kafka.SslEndpointIdentificationAlgorithm.Https, globalConfig.SslEndpointIdentificationAlgorithm);
            Assert.AreEqual("C:", globalConfig.SslKeyLocation);
            Assert.AreEqual("test", globalConfig.SslKeyPassword);
            Assert.AreEqual("pem", globalConfig.SslKeyPem);
            Assert.AreEqual("J:", globalConfig.SslKeystoreLocation);
            Assert.AreEqual("password", globalConfig.SslKeystorePassword);
            Assert.AreEqual("oepn", globalConfig.SslSigalgsList);
            Assert.AreEqual(14, globalConfig.StatisticsIntervalMs);
            Assert.AreEqual("*", globalConfig.TopicBlacklist);
            Assert.AreEqual(500, globalConfig.TopicMetadataRefreshFastIntervalMs);
            Assert.AreEqual(200, globalConfig.TopicMetadataRefreshIntervalMs);
            Assert.AreEqual(false, globalConfig.TopicMetadataRefreshSparse);
            #endregion

            #region AdminConfig
            Assert.AreEqual(Confluent.Kafka.Acks.All, adminConfig.Acks);
            Assert.AreEqual(1, adminConfig.ApiVersionFallbackMs);
            Assert.AreEqual(false, adminConfig.ApiVersionRequest);
            Assert.AreEqual(100, adminConfig.ApiVersionRequestTimeoutMs);
            Assert.AreEqual("127.0.0.1:9092", adminConfig.BootstrapServers);
            Assert.AreEqual(Confluent.Kafka.BrokerAddressFamily.V4, adminConfig.BrokerAddressFamily);
            Assert.AreEqual(100, adminConfig.BrokerAddressTtl);
            Assert.AreEqual("0.12.0", adminConfig.BrokerVersionFallback);
            Assert.AreEqual("admin", adminConfig.ClientId);
            Assert.AreEqual("1", adminConfig.ClientRack);
            Assert.AreEqual("all", adminConfig.Debug);
            Assert.AreEqual(true, adminConfig.EnableSaslOauthbearerUnsecureJwt);
            Assert.AreEqual(false, adminConfig.EnableSslCertificateVerification);
            Assert.AreEqual(1, adminConfig.InternalTerminationSignal);
            Assert.AreEqual(false, adminConfig.LogConnectionClose);
            Assert.AreEqual(true, adminConfig.LogQueue);
            Assert.AreEqual(false, adminConfig.LogThreadName);
            Assert.AreEqual(12, adminConfig.MaxInFlight);
            Assert.AreEqual(40, adminConfig.MessageCopyMaxBytes);
            Assert.AreEqual(500, adminConfig.MessageMaxBytes);
            Assert.AreEqual(6, adminConfig.MetadataMaxAgeMs);
            Assert.AreEqual(83, adminConfig.MetadataRequestTimeoutMs);
            Assert.AreEqual("D:", adminConfig.PluginLibraryPaths);
            Assert.AreEqual(1000, adminConfig.ReceiveMessageMaxBytes);
            Assert.AreEqual(9000, adminConfig.ReconnectBackoffMaxMs);
            Assert.AreEqual(8000, adminConfig.ReconnectBackoffMs);
            Assert.AreEqual("test", adminConfig.SaslKerberosKeytab);
            Assert.AreEqual("test", adminConfig.SaslKerberosKinitCmd);
            Assert.AreEqual(600, adminConfig.SaslKerberosMinTimeBeforeRelogin);
            Assert.AreEqual("Princiapl", adminConfig.SaslKerberosPrincipal);
            Assert.AreEqual("kerberos", adminConfig.SaslKerberosServiceName);
            Assert.AreEqual(Confluent.Kafka.SaslMechanism.ScramSha512, adminConfig.SaslMechanism);
            Assert.AreEqual("ouath", adminConfig.SaslOauthbearerConfig);
            Assert.AreEqual("test", adminConfig.SaslPassword);
            Assert.AreEqual("admin", adminConfig.SaslUsername);
            Assert.AreEqual(Confluent.Kafka.SecurityProtocol.SaslPlaintext, adminConfig.SecurityProtocol);
            Assert.AreEqual(true, adminConfig.SocketKeepaliveEnable);
            Assert.AreEqual(2, adminConfig.SocketMaxFails);
            Assert.AreEqual(true, adminConfig.SocketNagleDisable);
            Assert.AreEqual(50000, adminConfig.SocketReceiveBufferBytes);
            Assert.AreEqual(50000, adminConfig.SocketSendBufferBytes);
            Assert.AreEqual(6000, adminConfig.SocketTimeoutMs);
            Assert.AreEqual("D:", adminConfig.SslCaLocation);
            Assert.AreEqual("D:", adminConfig.SslCertificateLocation);
            Assert.AreEqual("D:", adminConfig.SslCertificatePem);
            Assert.AreEqual("ciphers", adminConfig.SslCipherSuites);
            Assert.AreEqual("D:", adminConfig.SslCrlLocation);
            Assert.AreEqual("", adminConfig.SslCurvesList);
            Assert.AreEqual(Confluent.Kafka.SslEndpointIdentificationAlgorithm.Https, adminConfig.SslEndpointIdentificationAlgorithm);
            Assert.AreEqual("C:", adminConfig.SslKeyLocation);
            Assert.AreEqual("test", adminConfig.SslKeyPassword);
            Assert.AreEqual("pem", adminConfig.SslKeyPem);
            Assert.AreEqual("J:", adminConfig.SslKeystoreLocation);
            Assert.AreEqual("password", adminConfig.SslKeystorePassword);
            Assert.AreEqual("oepn", adminConfig.SslSigalgsList);
            Assert.AreEqual(14, adminConfig.StatisticsIntervalMs);
            Assert.AreEqual("*", adminConfig.TopicBlacklist);
            Assert.AreEqual(500, adminConfig.TopicMetadataRefreshFastIntervalMs);
            Assert.AreEqual(200, adminConfig.TopicMetadataRefreshIntervalMs);
            Assert.AreEqual(false, adminConfig.TopicMetadataRefreshSparse);
            #endregion
        }
    }
}
