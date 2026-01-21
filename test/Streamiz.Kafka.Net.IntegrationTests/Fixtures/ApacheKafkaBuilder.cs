using System.Security.Principal;
using System.Text;
using Docker.DotNet.Models;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using Testcontainers.Kafka;

namespace Streamiz.Kafka.Net.IntegrationTests.Fixtures;

public class ApacheKafkaBuilder 
    : ContainerBuilder<ApacheKafkaBuilder, KafkaContainer, KafkaConfiguration>
{
    public const string DEFAULT_IMAGE_NAME = $"apache/kafka:{DOCKER_VERSION}";
    public const string APACHE_KAFKA_NATIVE_IMAGE_NAME = $"apache/kafka-native:{DOCKER_VERSION}";
    public const string DOCKER_VERSION = "4.1.0";
    
    public const string KafkaImage = DEFAULT_IMAGE_NAME;
    public const ushort KafkaPort = 9092;
    public const ushort BrokerPort = 9093;
    public const string StartupScriptFilePath = "/testcontainers.sh";
    
    private const string DEFAULT_CLUSTER_ID = "4L6g3nShT-eMCtK--X86sw";
        
    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaBuilder" /> class.
    /// </summary>
    public ApacheKafkaBuilder()
        : this(new KafkaConfiguration())
    {
        DockerResourceConfiguration = Init().DockerResourceConfiguration;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaBuilder" /> class.
    /// </summary>
    /// <param name="resourceConfiguration">The Docker resource configuration.</param>
    private ApacheKafkaBuilder(KafkaConfiguration resourceConfiguration)
        : base(resourceConfiguration)
    {
        DockerResourceConfiguration = resourceConfiguration;
    }

    /// <inheritdoc />
    protected override KafkaConfiguration DockerResourceConfiguration { get; }

    /// <inheritdoc />
    public override KafkaContainer Build()
    {
        Validate();
        return new KafkaContainer(DockerResourceConfiguration);
    }

    /// <inheritdoc />
    protected sealed override ApacheKafkaBuilder Init()
    {
        return base.Init()
            .WithImage(KafkaImage)
            .WithPortBinding(KafkaPort, true)
            .WithPortBinding(BrokerPort, true)
            .WithEnvironment("CLUSTER_ID", DEFAULT_CLUSTER_ID)
            .WithEnvironment("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:" + KafkaPort + ",BROKER://0.0.0.0:" + BrokerPort + ",CONTROLLER://0.0.0.0:9094")
            .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT")
            .WithEnvironment("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
            .WithEnvironment("KAFKA_PROCESS_ROLES", "broker,controller")
            .WithEnvironment("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .WithEnvironment("KAFKA_NODE_ID", "1")
            .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .WithEnvironment("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
            .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .WithEnvironment("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", long.MaxValue.ToString())
            .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .WithEnvironment("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9094")
            .WithEntrypoint("/bin/sh", "-c")
            .WithCommand("while [ ! -f " + StartupScriptFilePath + " ]; do sleep 0.1; done; " + StartupScriptFilePath)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("\\[KafkaRaftServer nodeId=\\d+\\] Kafka Server started"))
            .WithStartupCallback((container, ct) =>
            {
                const char lf = '\n';
                var startupScript = new StringBuilder();
                startupScript.Append("#!/bin/bash");
                startupScript.Append(lf);
                startupScript.Append("export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://" + container.Hostname + ":" + container.GetMappedPublicPort(KafkaPort) + ",BROKER://" + container.IpAddress + ":" + BrokerPort);
                startupScript.Append(lf);
                startupScript.Append("/etc/kafka/docker/run");
                return container.CopyAsync(Encoding.Default.GetBytes(startupScript.ToString()), StartupScriptFilePath, Unix.FileMode755, ct);
            });
    }

    /// <inheritdoc />
    protected override ApacheKafkaBuilder Clone(IResourceConfiguration<CreateContainerParameters> resourceConfiguration)
    {
        return Merge(DockerResourceConfiguration, new KafkaConfiguration(resourceConfiguration));
    }

    /// <inheritdoc />
    protected override ApacheKafkaBuilder Clone(IContainerConfiguration resourceConfiguration)
    {
        return Merge(DockerResourceConfiguration, new KafkaConfiguration(resourceConfiguration));
    }

    /// <inheritdoc />
    protected override ApacheKafkaBuilder Merge(KafkaConfiguration oldValue, KafkaConfiguration newValue)
    {
        return new ApacheKafkaBuilder(new KafkaConfiguration(oldValue, newValue));
    }
}