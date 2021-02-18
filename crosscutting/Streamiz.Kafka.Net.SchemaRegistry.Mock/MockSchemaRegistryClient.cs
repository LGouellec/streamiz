using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.SchemaRegistry.Mock
{
    /// <summary>
    /// Mock schema registry client. Implements <see cref="ISchemaRegistryClient"/>.
    /// </summary>
    public class MockSchemaRegistryClient : ISchemaRegistryClient
    {
        private class RegisterSchema
        {
            public string Schema { get; set; }
            public int Version { get; set; }
            public int Id { get; set; }
        }

        private int id = 0;
        private readonly List<string> subjects = new List<string>();
        private readonly Dictionary<string, List<RegisterSchema>> schemas = new Dictionary<string, List<RegisterSchema>>();

        #region ISchemaRegistryClient Impl
        public int MaxCachedSchemas => 100;

        public string ConstructKeySubjectName(string topic, string recordType = null)
            => $"{topic}-key";

        public string ConstructValueSubjectName(string topic, string recordType = null)
            => $"{topic}-value";

        public void Dispose() { }

        public Task<List<string>> GetAllSubjectsAsync()
            => Task.FromResult(subjects);

        public Task<RegisteredSchema> GetLatestSchemaAsync(string subject)
        {
            if (schemas.ContainsKey(subject))
            {
                var schema = schemas[subject];
                var s = schema.OrderByDescending(s => s.Version).FirstOrDefault();
                return s != null ? Task.FromResult(
                    new RegisteredSchema(
                        subject,
                        s.Version,
                        s.Id,
                        s.Schema,
                        SchemaType.Avro,
                        new List<SchemaReference>())
                    ) : Task.FromResult((RegisteredSchema)null);
            }
            else
            {
                return Task.FromResult((RegisteredSchema)null);
            }
        }

        public Task<RegisteredSchema> GetRegisteredSchemaAsync(string subject, int version)
        {
            if (schemas.ContainsKey(subject))
            {
                var schema = schemas[subject];
                var s = schema.FirstOrDefault(s => s.Version.Equals(version));
                return s != null ? Task.FromResult(
                    new RegisteredSchema(
                        subject,
                        s.Version,
                        s.Id,
                        s.Schema,
                        SchemaType.Avro,
                        new List<SchemaReference>())
                    ) : Task.FromResult((RegisteredSchema)null);
            }
            else
            {
                return Task.FromResult((RegisteredSchema)null);
            }
        }

        public Task<Schema> GetSchemaAsync(int id, string format = null)
        {
            var schema = schemas
                            .SelectMany(s => s.Value)
                            .FirstOrDefault(s => s.Id.Equals(id));
            return schema != null ?
                Task.FromResult(new Schema(schema.Schema, SchemaType.Avro)) :
                Task.FromResult((Schema)null);
        }

        public Task<string> GetSchemaAsync(string subject, int version)
        {
            if (schemas.ContainsKey(subject))
            {
                var schema = schemas[subject];
                var s = schema.FirstOrDefault(s => s.Version.Equals(version));
                return s != null ?
                    Task.FromResult(s.Schema) :
                    Task.FromResult((string)null);
            }
            else
            {
                return Task.FromResult((string)null);
            }
        }

        public Task<int> GetSchemaIdAsync(string subject, string avroSchema)
        {
            if (schemas.ContainsKey(subject))
            {
                var schema = schemas[subject];
                var s = schema.FirstOrDefault(s => s.Schema.Equals(avroSchema));
                return s != null ?
                    Task.FromResult(s.Id) :
                    Task.FromResult(-1);
            }
            else
            {
                return Task.FromResult(-1);
            }
        }

        public Task<int> GetSchemaIdAsync(string subject, Schema schema)
            => GetSchemaIdAsync(subject, schema.SchemaString);

        public Task<List<int>> GetSubjectVersionsAsync(string subject)
        {
            if (schemas.ContainsKey(subject))
            {
                var schema = schemas[subject];
                return Task.FromResult(schema.Select(s => s.Id).ToList());
            }
            else
            {
                return Task.FromResult(new List<int>());
            }
        }

        public Task<bool> IsCompatibleAsync(string subject, string avroSchema)
            => Task.FromResult(true);

        public Task<bool> IsCompatibleAsync(string subject, Schema schema)
            => IsCompatibleAsync(subject, schema.SchemaString);

        public Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool ignoreDeletedSchemas)
        {
            throw new NotImplementedException();
        }

        public Task<int> RegisterSchemaAsync(string subject, string avroSchema)
        {
            ++id;
            if (schemas.ContainsKey(subject))
            {
                var maxVersion = schemas[subject].Max(s => s.Version) + 1;
                schemas[subject].Add(new RegisterSchema
                {
                    Id = id,
                    Schema = avroSchema,
                    Version = maxVersion
                });
            }
            else
            {
                schemas.Add(subject, new List<RegisterSchema> {
                    new RegisterSchema {
                    Id = id,
                    Schema = avroSchema,
                    Version = 1
                    }
                });
                subjects.Add(subject);
            }
            return Task.FromResult(id);
        }

        public Task<int> RegisterSchemaAsync(string subject, Schema schema)
            => RegisterSchemaAsync(subject, schema.SchemaString);

        #endregion
    }
}