﻿namespace Streamiz.Kafka.Net
{
    /// <summary>
    ///  Subject name strategy. Refer to: https://www.confluent.io/blog/put-several-event-types-kafka-topic/
    /// </summary>
    public enum SubjectNameStrategy
    {
        /// <summary>
        ///     (default): The subject name for message keys is &lt;topic&gt;-key, and &lt;topic&gt;-value for message values.
        ///     This means that the schemas of all messages in the topic must be compatible with each other.
        /// </summary>
        Topic,

        /// <summary>
        ///     The subject name is the fully-qualified name of the Avro record type of the message.
        ///     Thus, the schema registry checks the compatibility for a particular record type, regardless of topic.
        ///     This setting allows any number of different event types in the same topic.
        /// </summary>
        Record,

        /// <summary>
        ///     The subject name is &lt;topic&gt;-&lt;type&gt;, where &lt;topic&gt; is the Kafka topic name, and &lt;type&gt;
        ///     is the fully-qualified name of the Avro record type of the message. This setting also allows any number of event
        ///     types in the same topic, and further constrains the compatibility check to the current topic only.
        /// </summary>
        TopicRecord
    }

    /// <summary>
    ///    Subject name strategy for referenced schemas.
    /// </summary>
    public enum ReferenceSubjectNameStrategy
    {
        /// <summary>
        ///    (default): Use the reference name as the subject name.
        /// </summary>
        ReferenceName
    }

    /// <summary>
    /// Interface schema registry configuration.
    /// Configure url schema registry, auto register configuration, etc .... for stream application.
    /// See <see cref="StreamConfig"/> to obtain implementation about this interface.
    /// </summary>
    public interface ISchemaRegistryConfig
    {
        /// <summary>
        /// A comma-separated list of URLs for schema registry instances that are used register or lookup schemas.
        /// </summary>
        public string SchemaRegistryUrl { get; set; }

        /// <summary>
        /// Specifies the timeout for requests to Confluent Schema Registry. default: 30000
        /// </summary>
        public int? SchemaRegistryRequestTimeoutMs { get; set; }

        /// <summary>
        /// Specifies the maximum number of schemas CachedSchemaRegistryClient should cache locally. default: 1000
        /// </summary>
        public int? SchemaRegistryMaxCachedSchemas { get; set; }

        /// <summary>
        /// Specifies whether or not the Avro serializer should attempt to auto-register unrecognized schemas with Confluent Schema Registry. default: true
        /// </summary>
        public bool? AutoRegisterSchemas { get; set; }

        /// <summary>
        /// The subject name strategy to use for schema registration / lookup. Possible values: <see cref="Streamiz.Kafka.Net.SubjectNameStrategy" />
        /// </summary>
        public SubjectNameStrategy? SubjectNameStrategy { get; set; }

        /// <summary>
        /// Credentials for the schema registry
        /// </summary>
        public string BasicAuthUserInfo { get; set; }

        /// <summary>
        /// Specifies the source, "USER_INFO" or "SASL_INHERIT".
        /// </summary>
        public string BasicAuthCredentialsSource { get; set; }

        /// <summary>
        ///    Specifies the initial size (in bytes) of the buffer used for message
        ///    serialization. Use a value high enough to avoid resizing the buffer, but small
        ///    enough to avoid excessive memory use. Inspect the size of the byte array returned
        ///    by the Serialize method to estimate an appropriate value. Note: each call to
        ///    serialize creates a new buffer. default: 1024
        /// </summary>
        public int? BufferBytes { get; set; }

        /// <summary>
        ///    Specifies whether or not the serializer should use the latest subject
        ///    version for serialization. WARNING: There is no check that the latest schema
        ///    is backwards compatible with the schema of the object being serialized. default:
        ///    false
        /// </summary>
        public bool? UseLatestVersion { get; set; }

        /// <summary>
        ///    Specifies whether or not the Protobuf serializer should skip known types when
        ///    resolving dependencies. default: false
        /// </summary>
        public bool? SkipKnownTypes { get; set; }

        /// <summary>
        ///    Specifies whether the Protobuf serializer should serialize message indexes without
        ///    zig-zag encoding. default: false
        /// </summary>
        public bool? UseDeprecatedFormat { get; set; }

        /// <summary>
        ///    Reference subject name strategy. default: ReferenceSubjectNameStrategy.ReferenceName
        /// </summary>
        public ReferenceSubjectNameStrategy? ReferenceSubjectNameStrategy { get; set; }
    }
}