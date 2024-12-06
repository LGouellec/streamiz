﻿using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SchemaRegistry.SerDes.Mock;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Streamiz.Kafka.Net.Tests,PublicKey=00240000048000009400000006020000002400005253413100040000010001000d9d4a8e90a3b987f68f047ec499e5a3405b46fcad30f52abadefca93b5ebce094d05976950b38cc7f0855f600047db0a351ede5e0b24b9d5f1de6c59ab55dee145da5d13bb86f7521b918c35c71ca5642fc46ba9b04d4900725a2d4813639ff47898e1b762ba4ccd5838e2dd1e1664bd72bf677d872c87749948b1174bd91ad")]
namespace Streamiz.Kafka.Net.SchemaRegistry.SerDes
{
    /// <summary>
    /// Abstract SerDes for use with schema registries
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="CS">Serializer config type</typeparam>
    /// <typeparam name="CD">Deserializer config type</typeparam>
    public abstract class SchemaSerDes<T, CS, CD> : AbstractSerDes<T>
        where CS : Config, new()
        where CD : Config, new()
    {
        private readonly string prefixConfig;

        protected SchemaSerDes(string prefixConfig)
        {
            this.prefixConfig = prefixConfig;
        }
        
        /// <summary>
        /// Schema registry client
        /// </summary>
        protected ISchemaRegistryClient registryClient;
        
        /// <summary>
        /// Serializer 
        /// </summary>
        protected IAsyncSerializer<T> serializer;
        
        /// <summary>
        /// Deserializer
        /// </summary>
        protected IAsyncDeserializer<T> deserializer;
        
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

            var b = serializer
                    .AsSyncOverAsync()
                    .Serialize(data, context);
            return b;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="config"></param>
        /// <returns></returns>
        protected virtual ISchemaRegistryClient GetSchemaRegistryClient(SchemaRegistryConfig config)
        {
            string mockScope = MaybeGetScope(config.Url);
            if (mockScope != null)
            {
                return MockSchemaRegistry.GetClientForScope(mockScope, config);
            }
            else
            {
                return new CachedSchemaRegistryClient(config);
            }
        }

        /// <summary>
        /// Transform <see cref="ISchemaRegistryConfig"/> to <see cref="SchemaRegistryConfig"/>
        /// </summary>
        /// <param name="config">Streamiz schema registry config</param>
        /// <param name="streamConfig">Streamiz config</param>
        /// <returns></returns>
        protected virtual SchemaRegistryConfig GetConfig(ISchemaRegistryConfig config, IStreamConfig streamConfig)
        {
            var c = new SchemaRegistryConfig
            {
                Url = config.SchemaRegistryUrl
            };

            if (config.SchemaRegistryMaxCachedSchemas.HasValue)
            {
                c.MaxCachedSchemas = config.SchemaRegistryMaxCachedSchemas;
            }

            if (config.SchemaRegistryRequestTimeoutMs.HasValue)
            {
                c.RequestTimeoutMs = config.SchemaRegistryRequestTimeoutMs;
            }

            if (!string.IsNullOrEmpty(config.BasicAuthUserInfo))
            {
                c.BasicAuthUserInfo = config.BasicAuthUserInfo;
            }

            if (!string.IsNullOrEmpty(config.BasicAuthCredentialsSource) && 
                Enum.TryParse(config.BasicAuthCredentialsSource, out AuthCredentialsSource credentialsSource))
            {
                c.BasicAuthCredentialsSource = credentialsSource;
            }

            var explicitStreamConfig = streamConfig.GetPrefixScan("schema.registry.");
            foreach (var kv in explicitStreamConfig)
            {
                try
                {
                    c.Set(kv.Key, kv.Value);
                }
                catch
                {
                    // ignored
                }
            }

            return c;
        }

        /// <summary>
        /// Transform <see cref="ISchemaRegistryConfig"/> to <typeparamref name="CS"/>
        /// </summary>
        /// <param name="config">Streamiz schema registry config</param>
        /// <param name="streamConfig">Streamiz config</param>
        /// <returns></returns>
        protected virtual CS GetSerializerConfig(ISchemaRegistryConfig config, IStreamConfig streamConfig)
        {
            string Key(string keySuffix) => $"{prefixConfig}.{keySuffix}";
            
            CS c = new CS();
            
            if (config.AutoRegisterSchemas.HasValue)
                c.Set(Key("serializer.auto.register.schemas"), config.AutoRegisterSchemas.ToString());

            if (config.SubjectNameStrategy.HasValue)
                c.Set(Key("serializer.subject.name.strategy"), ((Confluent.SchemaRegistry.SubjectNameStrategy) config.SubjectNameStrategy.Value).ToString());
            
            if (config.UseLatestVersion.HasValue)
                c.Set(Key("serializer.use.latest.version"), config.UseLatestVersion.ToString());
            
            if (config.BufferBytes.HasValue)
                c.Set(Key("serializer.buffer.bytes"), config.BufferBytes.ToString());

            var results = streamConfig.GetPrefixScan(Key("serializer."));
            foreach (var kv in results)
            {
                try
                {
                    c.Set(kv.Key, kv.Value);
                }
                catch
                {
                    // ignored
                }
            }
            
            return c;
        }
        
        /// <summary>
        /// Transform <see cref="ISchemaRegistryConfig"/> to <typeparamref name="CD"/>
        /// </summary>
        /// <param name="config">Streamiz schema registry config</param>
        /// <param name="streamConfig">Streamiz config</param>
        /// <returns></returns>
        protected virtual CD GetDeserializerConfig(ISchemaRegistryConfig config, IStreamConfig streamConfig)
        {
            string Key(string keySuffix) => $"{prefixConfig}.{keySuffix}";
            
            CD c = new CD();
            
            if (config.SubjectNameStrategy.HasValue)
                c.Set(Key("deserializer.subject.name.strategy"), ((Confluent.SchemaRegistry.SubjectNameStrategy) config.SubjectNameStrategy.Value).ToString());
            
            if (config.UseLatestVersion.HasValue)
                c.Set(Key("deserializer.use.latest.version"), config.UseLatestVersion.ToString());

            var results = streamConfig.GetPrefixScan(Key("deserializer."));
            foreach (var kv in results)
            {
                try
                {
                    c.Set(kv.Key, kv.Value);
                }
                catch
                {
                    // ignored
                }
            }
            
            return c;
        }
        
        #region Privates
        
        // FOR TESTING
        internal SchemaRegistryConfig ToConfig(ISchemaRegistryConfig config, IStreamConfig streamConfig)
            => GetConfig(config, streamConfig);
        internal CS ToSerializerConfig(ISchemaRegistryConfig config, IStreamConfig streamConfig)
            => GetSerializerConfig(config, streamConfig);
        internal CD ToDeserializerConfig(ISchemaRegistryConfig config, IStreamConfig streamConfig)
            => GetDeserializerConfig(config, streamConfig);
        
        private string MaybeGetScope(string schemaRegistryUrl)
        {
            IEnumerable<string> urls = schemaRegistryUrl != null ?
                schemaRegistryUrl.Split(',').ToList() :
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