namespace Streamiz.Kafka.Net
{
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
    }
}