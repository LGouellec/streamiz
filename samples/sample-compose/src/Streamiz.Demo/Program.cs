using Confluent.Kafka;
using Microsoft.AspNetCore.Builder;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Metrics.OpenTelemetry;

// This is required if the collector doesn't expose an https endpoint. By default, .NET
// only allows http2 (required for gRPC) to secure endpoints.
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

var builder = WebApplication.CreateBuilder(args);
IConfigurationSection config = builder.Configuration
    .GetRequiredSection("StreamConfig");

var sConfig = new StreamConfig
{
    ApplicationId = config["ApplicationId"],
    BootstrapServers = config["BootstrapServers"],
    SchemaRegistryUrl = config["SchemaRegistryUrl"],
    AutoOffsetReset = config["AutoOffsetReset"] is string aor
        ? Enum.Parse<AutoOffsetReset>(aor, true)
        : AutoOffsetReset.Earliest,
    AutoRegisterSchemas = config["AutoRegisterSchemas"] is string ars && bool.Parse(ars),
};

string otelUrl = builder.Configuration.GetValue<string>("OtelCollectorUrl")!;

sConfig.UseOpenTelemetryReporter((builder) =>
    {
        builder.AddHttpClientInstrumentation();
        builder.AddAspNetCoreInstrumentation();
        builder.AddOtlpExporter(options => options.Endpoint = new Uri(otelUrl));
    }, true);

builder.Services.AddSingleton(sConfig);
builder.Services.AddHostedService<Streamiz.Demo.KStream.WordSplitter>();

var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.Run();
