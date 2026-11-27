# Topology Introspection

`Topology.Describe()` gives a text description of a topology. Streamiz also exposes the same information in a machine-readable form, so operators, tooling and AI assistants can inspect a running application without reading its source code or logs:

- a JSON **topology model**, in the core package;
- an opt-in **HTTP endpoint**, in the `Streamiz.Kafka.Net.AspNetCore` package;
- read-only **MCP tools** (Model Context Protocol), in the `Streamiz.Kafka.Net.Mcp` package.

## Topology model

`TopologyModel` describes the sub-topologies, their nodes (source, processor, sink) with their predecessors and successors, the topics, the state stores and the global stores.

``` csharp
// From a topology (internal topics keep their logical name)
TopologyModel model = builder.Build().Describe().ToModel();

// From a running application: internal topics are resolved with the application id
TopologyModel model = stream.DescribeTopology();

string json = model.ToJson();   // indented JSON
string text = model.ToString(); // same output as Topology.Describe()
```

At the topology level, `sourceTopics` and `sinkTopics` list the input and output topics. Internal topics are listed apart, in `repartitionTopics` and `changelogTopics`, and are only resolved when the model comes from `KafkaStream.DescribeTopology()`.

## HTTP endpoint

Add the `Streamiz.Kafka.Net.AspNetCore` package, then map the endpoint. Nothing is exposed unless you call `MapStreamizTopology()`.

``` csharp
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton(kafkaStream);

var app = builder.Build();
app.MapStreamizTopology()             // GET /topology, uses the KafkaStream registered in the services
   .RequireAuthorization();           // optional, secure it like any other endpoint
// or app.MapStreamizTopology(kafkaStream, "/streamiz/topology");
// or app.MapStreamizTopology(topology);
```

The endpoint returns JSON by default, and the text description with `?format=text` or an `Accept: text/plain` header. It returns `503` when no `KafkaStream` is registered.

## MCP server

Add the `Streamiz.Kafka.Net.Mcp` package. It provides read-only tools for an MCP server built with the official [MCP C# SDK](https://github.com/modelcontextprotocol/csharp-sdk), so an AI assistant can ask an application what it reads, writes and stores.

| Tool | Description |
|---|---|
| `get_topology` | Whole topology, as JSON (default) or text (`format`) |
| `list_topics` | Source, sink, repartition and changelog topics |
| `list_state_stores` | State stores, with the sub-topologies and processors using them, and global stores |
| `list_sub_topologies` | Summary of each sub-topology |
| `get_sub_topology` | Full detail of one sub-topology (`id`) |
| `get_node` | One source, processor or sink node (`name`) |

All tools are annotated as read-only, idempotent and non-destructive. They never change the application.

Over stdio, for example when an MCP client starts your application:

``` csharp
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddSingleton(kafkaStream);
builder.Services
    .AddMcpServer()
    .WithStdioServerTransport()
    .WithStreamizTopologyTools(); // uses the KafkaStream registered in the services
```

When stdio is the MCP transport, standard output carries the protocol messages: send your application logs to standard error.

Over HTTP, with the `ModelContextProtocol.AspNetCore` package:

``` csharp
builder.Services
    .AddMcpServer()
    .WithHttpTransport()
    .WithStreamizTopologyTools(kafkaStream); // or .WithStreamizTopologyTools(topology)

app.MapMcp("/mcp").RequireAuthorization();
```

Like the HTTP endpoint, the MCP tools describe your topics and stores: secure the transport before exposing it outside your network.
