namespace Streamiz.Kafka.Net
{
    public interface ISchemaRegistryConfig
    {
        public string SchemaRegistryUrl { get; set; }
        public int? SchemaRegistryRequestTimeoutMs { get; set; }
        public int? SchemaRegistryMaxCachedSchemas { get; set; }
        public bool? AutoRegisterSchemas { get; set; }
    }
}
