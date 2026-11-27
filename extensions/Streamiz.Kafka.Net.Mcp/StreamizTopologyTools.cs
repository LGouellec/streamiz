using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using Newtonsoft.Json;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Mcp
{
    /// <summary>
    /// Read-only MCP tools exposing the topology of a Streamiz application:
    /// sub-topologies, nodes, topics and state stores.
    /// Register them with <see cref="Microsoft.Extensions.DependencyInjection.StreamizMcpServerBuilderExtensions"/>.
    /// Every tool returns JSON text (except <c>get_topology</c> with the <c>text</c> format).
    /// </summary>
    [McpServerToolType]
    public sealed class StreamizTopologyTools
    {
        private static readonly JsonSerializerSettings JsonSettings = new()
        {
            NullValueHandling = NullValueHandling.Ignore,
            Formatting = Formatting.Indented
        };

        private readonly StreamizTopologyProvider provider;
        private readonly IServiceProvider services;

        /// <summary>
        /// Create the tools (instantiated by the MCP server for each tool call).
        /// </summary>
        /// <param name="provider">topology provider</param>
        /// <param name="services">service provider of the current request</param>
        public StreamizTopologyTools(StreamizTopologyProvider provider, IServiceProvider services)
        {
            this.provider = Guard.NotNull(provider, nameof(provider));
            this.services = services;
        }

        private TopologyModel Topology => provider.GetTopology(services);

        private static string ToJson(object value) => JsonConvert.SerializeObject(value, JsonSettings);

        /// <summary>
        /// Get the whole topology.
        /// </summary>
        /// <param name="format"><c>json</c> (default) or <c>text</c></param>
        /// <returns>the topology in the requested format</returns>
        [McpServerTool(Name = "get_topology", Title = "Get topology", ReadOnly = true, Idempotent = true,
            Destructive = false, OpenWorld = false)]
        [Description("Get the whole topology of the Streamiz (Kafka Streams .NET) application: sub-topologies, " +
                     "nodes (source, processor, sink) with their parents and children, topics, state stores and global stores.")]
        public string GetTopology(
            [Description("Output format: 'json' (default, machine-readable) or 'text' (same output as Topology.Describe()).")]
            string format = "json")
        {
            var model = Topology;
            if (string.IsNullOrEmpty(format) || string.Equals(format, "json", StringComparison.OrdinalIgnoreCase))
                return model.ToJson();
            if (string.Equals(format, "text", StringComparison.OrdinalIgnoreCase))
                return model.ToString();
            throw new McpException($"Unsupported format '{format}'. Supported formats are 'json' and 'text'.");
        }

        /// <summary>
        /// List the topics used by the topology.
        /// </summary>
        /// <returns>JSON object with the source, sink, repartition and changelog topics</returns>
        [McpServerTool(Name = "list_topics", Title = "List topics", ReadOnly = true, Idempotent = true,
            Destructive = false, OpenWorld = false)]
        [Description("List the Kafka topics used by the application: input (source) topics, output (sink) topics, " +
                     "and internal repartition and changelog topics. Internal topics are resolved only when the " +
                     "topology comes from a running KafkaStream.")]
        public string ListTopics()
        {
            var model = Topology;
            return ToJson(new
            {
                sourceTopics = model.SourceTopics,
                sinkTopics = model.SinkTopics,
                repartitionTopics = model.RepartitionTopics,
                changelogTopics = model.ChangelogTopics
            });
        }

        /// <summary>
        /// List the state stores used by the topology.
        /// </summary>
        /// <returns>JSON array of state stores</returns>
        [McpServerTool(Name = "list_state_stores", Title = "List state stores", ReadOnly = true, Idempotent = true,
            Destructive = false, OpenWorld = false)]
        [Description("List the state stores of the application, with the sub-topologies and processors using them. " +
                     "Global stores are flagged with 'global: true' and give their source topic.")]
        public string ListStateStores()
        {
            var model = Topology;

            var local = model.SubTopologies
                .SelectMany(s => s.Nodes
                    .Where(n => n.Stores != null)
                    .SelectMany(n => n.Stores.Select(store => (store, subTopology: s.Id, processor: n.Name))))
                .GroupBy(x => x.store)
                .Select(g => new
                {
                    name = g.Key,
                    global = false,
                    subTopologies = g.Select(x => x.subTopology).Distinct().OrderBy(id => id).ToList(),
                    processors = g.Select(x => x.processor).Distinct().OrderBy(p => p).ToList(),
                    topic = (string)null
                });

            var global = model.GlobalStores
                .Where(g => g.Store != null)
                .Select(g => new
                {
                    name = g.Store,
                    global = true,
                    subTopologies = new List<int>(),
                    processors = g.Processor != null ? new List<string> { g.Processor.Name } : new List<string>(),
                    topic = g.Topic
                });

            return ToJson(local.Concat(global).OrderBy(s => s.name).ToList());
        }

        /// <summary>
        /// List the sub-topologies with a summary of each one.
        /// </summary>
        /// <returns>JSON array of sub-topology summaries</returns>
        [McpServerTool(Name = "list_sub_topologies", Title = "List sub-topologies", ReadOnly = true, Idempotent = true,
            Destructive = false, OpenWorld = false)]
        [Description("List the sub-topologies of the application with, for each one, its id, node names, " +
                     "source and sink topics and state stores. Use get_sub_topology for the full detail of one.")]
        public string ListSubTopologies()
            => ToJson(Topology.SubTopologies.Select(s => new
            {
                id = s.Id,
                nodes = s.Nodes.Select(n => n.Name).ToList(),
                sourceTopics = s.SourceTopics,
                sinkTopics = s.SinkTopics,
                stateStores = s.StateStores
            }).ToList());

        /// <summary>
        /// Get one sub-topology.
        /// </summary>
        /// <param name="id">sub-topology id</param>
        /// <returns>JSON of the sub-topology</returns>
        [McpServerTool(Name = "get_sub_topology", Title = "Get sub-topology", ReadOnly = true, Idempotent = true,
            Destructive = false, OpenWorld = false)]
        [Description("Get the full detail of one sub-topology: its nodes with their type, topics, stores, " +
                     "successors and predecessors.")]
        public string GetSubTopology(
            [Description("Sub-topology id, as returned by list_sub_topologies.")]
            int id)
        {
            var model = Topology;
            var sub = model.SubTopologies.FirstOrDefault(s => s.Id == id)
                      ?? throw new McpException(
                          $"Unknown sub-topology {id}. Existing sub-topologies: {string.Join(", ", model.SubTopologies.Select(s => s.Id))}.");
            return ToJson(sub);
        }

        /// <summary>
        /// Get one node (source, processor or sink) by name.
        /// </summary>
        /// <param name="name">node name</param>
        /// <returns>JSON of the node with its sub-topology id</returns>
        [McpServerTool(Name = "get_node", Title = "Get node", ReadOnly = true, Idempotent = true,
            Destructive = false, OpenWorld = false)]
        [Description("Get one node of the topology (source, processor or sink) by name, with the sub-topology " +
                     "it belongs to (or the global store), its topics, stores, successors and predecessors.")]
        public string GetNode(
            [Description("Node name, as it appears in get_topology or list_sub_topologies.")]
            string name)
        {
            var model = Topology;

            foreach (var sub in model.SubTopologies)
            {
                var node = sub.Nodes.FirstOrDefault(n => n.Name == name);
                if (node != null)
                    return ToJson(new { subTopology = sub.Id, node });
            }

            foreach (var global in model.GlobalStores)
            {
                var node = new[] { global.Source, global.Processor }.FirstOrDefault(n => n != null && n.Name == name);
                if (node != null)
                    return ToJson(new { globalStore = global.Id, node });
            }

            throw new McpException($"Unknown node '{name}'.");
        }
    }
}
