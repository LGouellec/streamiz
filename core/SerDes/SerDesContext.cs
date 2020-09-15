namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// <see cref="SerDesContext"/> is using for configure <see cref="ISerDes"/> instance.
    /// It is notably used SchemaAvroSerDes to configure ISchemaRegistryClient with url, auto registry schema, etc ..
    /// </summary>
    public class SerDesContext
    {
        /// <summary>
        /// Stream application configuration instance
        /// </summary>
        public IStreamConfig Config { get; private set; }

        internal SerDesContext(IStreamConfig config)
        {
            Config = config;
        }
    }
}
