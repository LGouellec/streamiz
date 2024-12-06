using Confluent.SchemaRegistry;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.SchemaRegistry.SerDes.Mock
{
    /// <summary>
    /// Mock schema registry. Save all current mock registry client by scope.
    /// This class is thread-safe.
    /// If you want remove mock registry client in cache, you can call <see cref="DropScope(string)"/>.
    /// </summary>
    public class MockSchemaRegistry
    {
        private readonly static object _lock = new object();

        private readonly static IDictionary<string, ISchemaRegistryClient> scopedClients
            = new Dictionary<string, ISchemaRegistryClient>();

        /// <summary>
        /// Get client by scope. If client doesn't not exist, it will be create.
        /// </summary>
        /// <param name="scope">Client scope</param>
        /// <param name="config">Current schema registry configuration</param>
        /// <returns>Return a mock schema registry client</returns>
        public static ISchemaRegistryClient GetClientForScope(string scope, SchemaRegistryConfig config)
        {
            lock (_lock)
            {
                if (!scopedClients.ContainsKey(scope))
                {
                    MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
                    mockSchemaRegistryClient.UseConfiguration(config);
                    scopedClients.Add(scope, mockSchemaRegistryClient);
                }

                return scopedClients[scope];
            }
        }

        /// <summary>
        /// Drop scope if exists.
        /// </summary>
        /// <param name="scope">Client scope</param>
        public static void DropScope(string scope)
        {
            lock (_lock)
            {
                scopedClients.Remove(scope);
            }
        }

        /// <summary>
        /// Drop all scopes.
        /// </summary>
        public static void DropAllScope()
        {
            lock (_lock)
            {
                scopedClients.Clear();
            }
        }
    }
}