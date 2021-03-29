using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Mock;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.SchemaRegistry.SerDes
{
    /// <summary>
    /// Abstract SerDes for use with schema registries
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class SchemaSerDes<T> : AbstractSerDes<T>
    {
        protected ISchemaRegistryClient registryClient;
        protected IAsyncSerializer<T> serializer;
        protected IAsyncDeserializer<T> deserializer;

        private Confluent.SchemaRegistry.SchemaRegistryConfig GetConfig(ISchemaRegistryConfig config)
        {
            Confluent.SchemaRegistry.SchemaRegistryConfig c = new Confluent.SchemaRegistry.SchemaRegistryConfig();
            c.Url = config.SchemaRegistryUrl;
            if (config.SchemaRegistryMaxCachedSchemas.HasValue)
            {
                c.MaxCachedSchemas = config.SchemaRegistryMaxCachedSchemas;
            }

            if (config.SchemaRegistryRequestTimeoutMs.HasValue)
            {
                c.RequestTimeoutMs = config.SchemaRegistryRequestTimeoutMs;
            }

            return c;
        }

        /// <summary>
        /// Deserialize a record value from a byte array into <typeparamref name="T"/> value
        /// </summary>
        /// <param name="data">serialized bytes.</param>
        /// <param name="context">serialization context</param>
        /// <returns>deserialized <typeparamref name="T"/> using data; may be null</returns>
        public override T Deserialize(byte[] data, SerializationContext context)
        {
            if (!isInitialized)
            {
                throw new StreamsException($"SchemaSerDes<{typeof(T).Name}> is not initialized !");
            }

            return deserializer
                   .AsSyncOverAsync()
                   .Deserialize(data, data == null, context);
        }

        /// <summary>
        /// Convert <typeparamref name="T"/> <code>data</code> into a byte array.
        /// </summary>
        /// <param name="data"><typeparamref name="T"/> data</param>
        /// <param name="context">serialization context</param>
        /// <returns>serialized bytes</returns>
        public override byte[] Serialize(T data, SerializationContext context)
        {
            if (!isInitialized)
            {
                throw new StreamsException($"SchemaSerDes<{typeof(T).Name}> is not initialized !");
            }

            return serializer
                    .AsSyncOverAsync()
                    .Serialize(data, context);
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="config"></param>
        /// <returns></returns>
        protected virtual ISchemaRegistryClient GetSchemaRegistryClient(Confluent.SchemaRegistry.SchemaRegistryConfig config)
        {
            string mockScope = MaybeGetScope(config.Url);
            if (mockScope != null)
            {
                return MockSchemaRegistry.GetClientForScope(mockScope);
            }
            else
            {
                return new CachedSchemaRegistryClient(config);
            }
        }

        #region Privates

        private string MaybeGetScope(string schemaRegistryUrl)
        {
            IEnumerable<string> urls = schemaRegistryUrl != null ?
                schemaRegistryUrl.Split(",").ToList() :
                Enumerable.Empty<string>();
            List<string> scope = new List<string>();

            foreach (var url in urls)
                if (url.StartsWith("mock://"))
                    scope.Add(url.Replace("mock://", string.Empty));

            if (!scope.Any())
                return null;
            else if (scope.Count > 1)
                throw new ArgumentException($"Only one mock scope is permitted for 'schema.registry.url'. Got: {schemaRegistryUrl}");
            else if (urls.Count() > scope.Count)
                throw new ArgumentException($"Cannot mix mock and real urls for 'schema.registry.url'. Got: {schemaRegistryUrl}");
            else
                return scope.First();
        }

        #endregion Privates
    }
}