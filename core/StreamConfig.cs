using Confluent.Kafka;
using kafka_stream_core.Crosscutting;
using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Security;
using System.Text;

namespace kafka_stream_core
{
    public interface IStreamConfig
    {
        #region Methods 

        ProducerConfig ToProducerConfig();

        ConsumerConfig ToConsumerConfig();

        ConsumerConfig ToConsumerConfig(string clientid);

        ConsumerConfig ToGlobalConsumerConfig(string clientId);

        AdminClientConfig ToAdminConfig(string clientId);

        #endregion

        #region Stream Config Property

        /// <summary>
        /// An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.
        /// </summary>
        string ApplicationId { get; }

        /// <summary>
        /// An ID prefix string used for the client IDs of internal consumer, producer and restore-consumer, with pattern '<client.id>-StreamThread-<threadSequenceNumber>-<consumer|producer|restore-consumer>'.
        /// </summary>
        string ClientId { get; }

        int NumStreamThreads { get; }

        /// <summary>
        /// Default key serdes for consumer and materialized state store
        /// </summary>
        ISerDes DefaultKeySerDes { get; }

        /// <summary>
        /// Default value serdes for consumer and materialized state store
        /// </summary>
        ISerDes DefaultValueSerDes { get; }

        /// <summary>
        /// AutoOffsetReset
        /// </summary>
        AutoOffsetReset AutoOffsetReset { get; }

        #endregion
    }

    public class StreamConfig : Dictionary<string, dynamic>, IStreamConfig
    {
        #region Not used for moment

        private string applicationServerCst = "application.server";
        private string topologyOptimizationCst = "topology.optimization";
        private string cacheMaxBytesBufferingCst = "cache.max.bytes.buffering";
        private string rocksdbConfigSetterCst = "rocksdb.config.setter";
        private string stateCleanupDelayMsCst = "state.cleanup.delay.ms";
        private string pollMsCst = "poll.ms";
        private string processingGuaranteeCst = "processing.guarantee";

        public static readonly string AT_LEAST_ONCE = "at_least_once";
        public static readonly string EXACTLY_ONCE = "exactly_once";

        //private string Optimize
        //{
        //    get => this[topologyOptimizationCst];
        //    set => this.AddOrUpdate(topologyOptimizationCst, value);
        //}

        //private string ApplicationServer
        //{
        //    get => this[applicationServerCst];
        //    set => this.AddOrUpdate(applicationServerCst, value);
        //}

        //private string ProcessingGuaranteeConfig
        //{
        //    get => this[processingGuaranteeCst];
        //    set
        //    {
        //        if (value.Equals(AT_LEAST_ONCE) || value.Equals(EXACTLY_ONCE))
        //            this.AddOrUpdate(processingGuaranteeCst, value);
        //        else
        //            throw new InvalidOperationException($"ProcessingGuaranteeConfig value must equal to {AT_LEAST_ONCE} or {EXACTLY_ONCE}");
        //    }
        //}

        //private long PollMsConfig
        //{
        //    get => Convert.ToInt64(this[pollMsCst]);
        //    set => this.AddOrUpdate(pollMsCst, value.ToString());
        //}

        //private long StateCleanupDelayMs
        //{
        //    get => Convert.ToInt64(this[stateCleanupDelayMsCst]);
        //    set => this.AddOrUpdate(stateCleanupDelayMsCst, value.ToString());
        //}

        //private long CacheMaxBytesBuffering
        //{
        //    get => Convert.ToInt64(this[cacheMaxBytesBufferingCst]);
        //    set => this.AddOrUpdate(cacheMaxBytesBufferingCst, value.ToString());
        //}

        #endregion

        private string applicatonIdCst = "application.id";
        private string clientIdCst = "client.id";
        private string numStreamThreadsCst = "num.stream.threads";
        private string defaultKeySerDesCst = "default.key.serdes";
        private string defaultValueSerDesCst = "default.value.serdes";
        private string autoOffsetResetCst = "auto.offset.reset";

        #region IStreamConfig Property

        public AutoOffsetReset AutoOffsetReset
        {
            get => this[autoOffsetResetCst];
            set => this.AddOrUpdate(autoOffsetResetCst, value);
        }

        public int NumStreamThreads
        {
            get => this[numStreamThreadsCst];
            set => this.AddOrUpdate(numStreamThreadsCst, value);
        }

        public string ClientId
        {
            get => this[clientIdCst];
            set => this.AddOrUpdate(clientIdCst, value);
        }

        public string ApplicationId
        {
            get => this[applicatonIdCst];
            set => this.AddOrUpdate(applicatonIdCst, value);
        }

        public ISerDes DefaultKeySerDes
        {
            get => this[defaultKeySerDesCst];
            set => this.AddOrUpdate(defaultKeySerDesCst, value);
        }

        public ISerDes DefaultValueSerDes
        {
            get => this[defaultValueSerDesCst];
            set => this.AddOrUpdate(defaultValueSerDesCst, value);
        }

        #endregion

        #region Ctor

        public StreamConfig()
        {
            NumStreamThreads = 1;
            DefaultKeySerDes = new ByteArraySerDes();
            DefaultValueSerDes = new ByteArraySerDes();
            AutoOffsetReset = AutoOffsetReset.Earliest;
        }

        public StreamConfig(IDictionary<string, dynamic> properties)
            : this()
        {
            foreach (var k in properties)
                DictionaryExtensions.AddOrUpdate(this, k.Key, k.Value);
        }

        #endregion

        #region IStreamConfig Impl

        public ProducerConfig ToProducerConfig()
        {
            return new ProducerConfig
            {
                BootstrapServers = this["bootstrap.servers"],
                SaslMechanism = (SaslMechanism)Enum.Parse(typeof(SaslMechanism), this["sasl.mechanism"]),
                SaslUsername = this["sasl.username"],
                SaslPassword = this["sasl.password"],
                SecurityProtocol = SecurityProtocol.SaslPlaintext
            };
        }

        public ConsumerConfig ToConsumerConfig()
        {
            return new ConsumerConfig
            {
                BootstrapServers = this["bootstrap.servers"],
                SaslMechanism = (SaslMechanism)Enum.Parse(typeof(SaslMechanism), this["sasl.mechanism"]),
                SaslUsername = this["sasl.username"],
                SaslPassword = this["sasl.password"],
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                GroupId = this.ApplicationId,
                EnableAutoCommit = false,
                AutoOffsetReset = this.AutoOffsetReset
            };
        }

        public ConsumerConfig ToConsumerConfig(string clientId)
        {
            var config = this.ToConsumerConfig();
            config.ClientId = clientId;
            return config;
        }

        public ConsumerConfig ToGlobalConsumerConfig(string clientId)
        {
            // TODO
            return new ConsumerConfig();
        }

        public AdminClientConfig ToAdminConfig(string clientId)
        {
            // TODO
            return new AdminClientConfig();
        }

        #endregion
    }

    public class StreamConfig<KS, VS> : StreamConfig
        where KS : ISerDes, new()
        where VS : ISerDes, new()
    {
        public StreamConfig()
            :base()
        {
            DefaultKeySerDes = new KS();
            DefaultValueSerDes = new VS();
        }
    }
}