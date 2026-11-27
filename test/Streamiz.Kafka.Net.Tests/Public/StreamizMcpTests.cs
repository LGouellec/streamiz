using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ModelContextProtocol;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mcp;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class StreamizMcpTests
    {
        private const string ApplicationId = "test-mcp";

        private static readonly string[] ExpectedTools =
        {
            "get_node", "get_sub_topology", "get_topology", "list_state_stores", "list_sub_topologies", "list_topics"
        };

        // global store (sub-topology 0), sub-topology 1: source -> group by -> repartition, sub-topology 2: count (store) -> sink
        private static Topology BuildTopology()
        {
            var builder = new StreamBuilder();
            builder.GlobalTable("global-topic", InMemory.As<string, string>("global-store"));
            builder.Stream<string, string>("topic", new StringSerDes(), new StringSerDes(), "source")
                .GroupBy((k, _, _) => k.ToUpper())
                .Count(InMemory.As<string, long>("count-store"))
                .ToStream()
                .To("output");
            return builder.Build();
        }

        private static KafkaStream BuildStream()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = ApplicationId,
                BootstrapServers = "127.0.0.1"
            };
            return new KafkaStream(BuildTopology(), config, new SyncKafkaSupplier());
        }

        private static StreamizTopologyTools Tools(TopologyModel model)
            => new(new StreamizTopologyProvider(_ => model), null);

        #region Tools

        [Test]
        public void GetTopologyJsonAndText()
        {
            using var stream = BuildStream();
            var model = stream.DescribeTopology();
            var tools = Tools(model);

            var json = JObject.Parse(tools.GetTopology());
            Assert.AreEqual(2, ((JArray)json["subTopologies"]).Count);
            Assert.AreEqual(json.ToString(), JObject.Parse(tools.GetTopology("JSON")).ToString());

            Assert.AreEqual(model.ToString(), tools.GetTopology("text"));
            StringAssert.StartsWith("Topologies:", tools.GetTopology("text"));

            var e = Assert.Throws<McpException>(() => tools.GetTopology("xml"));
            StringAssert.Contains("xml", e.Message);
        }

        [Test]
        public void ListTopicsResolvesInternalTopics()
        {
            using var stream = BuildStream();
            var topics = JObject.Parse(Tools(stream.DescribeTopology()).ListTopics());

            CollectionAssert.AreEquivalent(new[] { "global-topic", "topic" }, topics["sourceTopics"].Values<string>());
            CollectionAssert.AreEqual(new[] { "output" }, topics["sinkTopics"].Values<string>());
            Assert.AreEqual(1, topics["repartitionTopics"].Count());
            StringAssert.StartsWith(ApplicationId + "-", topics["repartitionTopics"][0].Value<string>());
            CollectionAssert.AreEqual(new[] { ApplicationId + "-count-store-changelog" }, topics["changelogTopics"].Values<string>());

            // nodes reading/writing the repartition topic use its real name
            var repartition = topics["repartitionTopics"][0].Value<string>();
            var subs = JArray.Parse(Tools(stream.DescribeTopology()).ListSubTopologies());
            CollectionAssert.AreEqual(new[] { repartition }, subs[0]["sinkTopics"].Values<string>());
            CollectionAssert.AreEqual(new[] { repartition }, subs[1]["sourceTopics"].Values<string>());
        }

        [Test]
        public void ListStateStores()
        {
            var stores = JArray.Parse(Tools(BuildTopology().Describe().ToModel()).ListStateStores());

            Assert.AreEqual(2, stores.Count);

            var count = stores.Single(s => s["name"].Value<string>() == "count-store");
            Assert.IsFalse(count["global"].Value<bool>());
            CollectionAssert.AreEqual(new[] { 2 }, count["subTopologies"].Values<int>());
            Assert.AreEqual(1, count["processors"].Count());
            Assert.IsNull(count["topic"]);

            var global = stores.Single(s => s["name"].Value<string>() == "global-store");
            Assert.IsTrue(global["global"].Value<bool>());
            Assert.AreEqual("global-topic", global["topic"].Value<string>());
            Assert.AreEqual(1, global["processors"].Count());
            Assert.IsEmpty(global["subTopologies"]);
        }

        [Test]
        public void ListAndGetSubTopologies()
        {
            var tools = Tools(BuildTopology().Describe().ToModel());

            var subs = JArray.Parse(tools.ListSubTopologies());
            CollectionAssert.AreEqual(new[] { 1, 2 }, subs.Select(s => s["id"].Value<int>()));
            CollectionAssert.Contains(subs[0]["nodes"].Values<string>(), "source");
            CollectionAssert.AreEqual(new[] { "topic" }, subs[0]["sourceTopics"].Values<string>());
            CollectionAssert.AreEqual(new[] { "count-store" }, subs[1]["stateStores"].Values<string>());

            var sub = JObject.Parse(tools.GetSubTopology(2));
            Assert.AreEqual(2, sub["id"].Value<int>());
            Assert.IsTrue(sub["nodes"].Any(n => n["type"].Value<string>() == TopologyNodeType.Sink
                                                && n["topic"].Value<string>() == "output"));

            var e = Assert.Throws<McpException>(() => tools.GetSubTopology(42));
            StringAssert.Contains("42", e.Message);
            StringAssert.Contains("1, 2", e.Message);
        }

        [Test]
        public void GetNode()
        {
            var model = BuildTopology().Describe().ToModel();
            var tools = Tools(model);

            var source = JObject.Parse(tools.GetNode("source"));
            Assert.AreEqual(1, source["subTopology"].Value<int>());
            Assert.AreEqual(TopologyNodeType.Source, source["node"]["type"].Value<string>());
            CollectionAssert.AreEqual(new[] { "topic" }, source["node"]["topics"].Values<string>());

            var globalProcessor = model.GlobalStores.Single().Processor.Name;
            var global = JObject.Parse(tools.GetNode(globalProcessor));
            Assert.AreEqual(model.GlobalStores.Single().Id, global["globalStore"].Value<int>());
            Assert.IsNull(global["subTopology"]);

            Assert.Throws<McpException>(() => tools.GetNode("unknown"));
        }

        [Test]
        public void NoKafkaStreamRegistered()
        {
            var tools = new StreamizTopologyTools(
                new StreamizTopologyProvider(sp => sp?.GetService<KafkaStream>()?.DescribeTopology()),
                new ServiceCollection().BuildServiceProvider());

            var e = Assert.Throws<McpException>(() => tools.ListTopics());
            StringAssert.Contains(nameof(KafkaStream), e.Message);
        }

        [Test]
        public void NullArguments()
        {
            var builder = new ServiceCollection().AddMcpServer();

            Assert.Throws<ArgumentNullException>(() => new StreamizTopologyProvider(null));
            Assert.Throws<ArgumentNullException>(() => new StreamizTopologyTools(null, null));
            Assert.Throws<ArgumentNullException>(() => builder.WithStreamizTopologyTools((KafkaStream)null));
            Assert.Throws<ArgumentNullException>(() => builder.WithStreamizTopologyTools((Topology)null));
            Assert.Throws<ArgumentNullException>(() => builder.WithStreamizTopologyTools((StreamizTopologyProvider)null));
            Assert.Throws<ArgumentNullException>(() => ((IMcpServerBuilder)null).WithStreamizTopologyTools(BuildTopology()));
        }

        #endregion

        #region End to end (MCP client <-> MCP server)

        private sealed class McpSession : IAsyncDisposable
        {
            private readonly IHost host;
            public McpClient Client { get; }

            private McpSession(IHost host, McpClient client)
            {
                this.host = host;
                Client = client;
            }

            public static async Task<McpSession> StartAsync(Action<IServiceCollection> configureServices,
                Action<IMcpServerBuilder> configureMcp)
            {
                var clientToServer = new Pipe();
                var serverToClient = new Pipe();

                var builder = Host.CreateApplicationBuilder();
                builder.Logging.ClearProviders();
                configureServices(builder.Services);
                var mcp = builder.Services
                    .AddMcpServer()
                    .WithStreamServerTransport(clientToServer.Reader.AsStream(), serverToClient.Writer.AsStream());
                configureMcp(mcp);

                var host = builder.Build();
                await host.StartAsync();

                var client = await McpClient.CreateAsync(
                    new StreamClientTransport(clientToServer.Writer.AsStream(), serverToClient.Reader.AsStream()));
                return new McpSession(host, client);
            }

            public async Task<string> CallAsync(string tool, Dictionary<string, object> arguments = null)
            {
                var result = await Client.CallToolAsync(tool, arguments);
                var text = string.Concat(result.Content.OfType<TextContentBlock>().Select(c => c.Text));
                if (result.IsError == true)
                    throw new McpException(text);
                return text;
            }

            public async ValueTask DisposeAsync()
            {
                await Client.DisposeAsync();
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await host.StopAsync(cts.Token);
                host.Dispose();
            }
        }

        [Test]
        public async Task EndToEndWithRegisteredKafkaStream()
        {
            using var stream = BuildStream();
            await using var session = await McpSession.StartAsync(
                services => services.AddSingleton(stream),
                mcp => mcp.WithStreamizTopologyTools());

            var tools = await session.Client.ListToolsAsync();
            CollectionAssert.AreEqual(ExpectedTools, tools.Select(t => t.Name).OrderBy(n => n));
            foreach (var tool in tools)
            {
                var annotations = tool.ProtocolTool.Annotations;
                Assert.IsTrue(annotations.ReadOnlyHint, tool.Name);
                Assert.IsFalse(annotations.DestructiveHint, tool.Name);
                Assert.IsFalse(string.IsNullOrEmpty(tool.Description), tool.Name);
            }

            var topics = JObject.Parse(await session.CallAsync("list_topics"));
            CollectionAssert.AreEqual(new[] { ApplicationId + "-count-store-changelog" }, topics["changelogTopics"].Values<string>());

            var text = await session.CallAsync("get_topology", new Dictionary<string, object> { ["format"] = "text" });
            StringAssert.StartsWith("Topologies:", text);

            var sub = JObject.Parse(await session.CallAsync("get_sub_topology", new Dictionary<string, object> { ["id"] = 2 }));
            Assert.AreEqual(2, sub["id"].Value<int>());

            var node = JObject.Parse(await session.CallAsync("get_node", new Dictionary<string, object> { ["name"] = "source" }));
            Assert.AreEqual("source", node["node"]["name"].Value<string>());
        }

        [Test]
        public async Task EndToEndWithoutKafkaStreamReturnsToolError()
        {
            await using var session = await McpSession.StartAsync(_ => { }, mcp => mcp.WithStreamizTopologyTools());

            var result = await session.Client.CallToolAsync("list_topics");
            Assert.IsTrue(result.IsError);
            StringAssert.Contains(nameof(KafkaStream),
                string.Concat(result.Content.OfType<TextContentBlock>().Select(c => c.Text)));
        }

        [Test]
        public async Task EndToEndWithTopologyAndKafkaStreamInstances()
        {
            await using (var session = await McpSession.StartAsync(_ => { }, mcp => mcp.WithStreamizTopologyTools(BuildTopology())))
            {
                var stores = JArray.Parse(await session.CallAsync("list_state_stores"));
                Assert.AreEqual(2, stores.Count);
                // internal topics are not resolved without an application id
                Assert.IsEmpty(JObject.Parse(await session.CallAsync("list_topics"))["changelogTopics"]);
            }

            using var stream = BuildStream();
            await using (var session = await McpSession.StartAsync(_ => { }, mcp => mcp.WithStreamizTopologyTools(stream)))
            {
                var subs = JArray.Parse(await session.CallAsync("list_sub_topologies"));
                Assert.AreEqual(2, subs.Count);
                Assert.AreEqual(1, JObject.Parse(await session.CallAsync("list_topics"))["repartitionTopics"].Count());
            }
        }

        #endregion
    }
}
