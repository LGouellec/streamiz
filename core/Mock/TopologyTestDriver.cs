using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock
{
    /// <summary>
    /// This class makes it easier to write tests to verify the behavior of topologies created with <see cref="Topology"/> or
    /// <see cref="StreamBuilder" />.
    /// <para>
    /// <see cref="TopologyTestDriver"/> is <see cref="IDisposable"/>. Be warning to dispose or use <code>using</code> keyword in your unit tests.
    /// </para>
    /// You can test simple topologies that have a single processor, or very complex topologies that have multiple sources,
    /// processors, sinks, or sub-topologies.
    /// Best of all, the class works without a real Kafka broker, so the tests execute very quickly with very little overhead.
    /// <p>
    /// Using the <see cref="TopologyTestDriver"/> in tests is easy: simply instantiate the driver and provide a <see cref="Topology"/>
    /// (cf. <see cref="StreamBuilder.Build()"/>) and <see cref="IStreamConfig"/>, <see cref="CreateInputTopic{K, V}(string, ISerDes{K}, ISerDes{V})"/>
    /// and use a <see cref="TestInputTopic{K, V}"/> to supply an input records to the topology,
    /// and then <see cref="CreateOuputTopic{K, V}(string, TimeSpan, ISerDes{K}, ISerDes{V})"/> and use a <see cref="TestOutputTopic{K, V}"/> to read and
    /// verify any output records by the topology.
    /// </p>
    /// <p>
    /// Although the driver doesn't use a real Kafka broker, it does simulate Kafka Cluster in memory <see cref="MockConsumer"/> and
    /// <see cref="MockProducer"/> that read and write raw {@code byte[]} messages.
    /// </p>
    /// <example>
    /// Driver setup
    /// <code>
    /// static void Main(string[] args)
    /// {
    ///     var config = new StreamConfig&lt;StringSerDes, StringSerDes&gt;();
    ///     config.ApplicationId = "test-test-driver-app";
    ///     
    ///     StreamBuilder builder = new StreamBuilder();
    /// 
    ///     builder.Stream&lt;string, string&gt;("test").Filter((k, v) => v.Contains("test")).To("test-output");
    /// 
    ///     Topology t = builder.Build();
    /// 
    ///     using (var driver = new TopologyTestDriver(t, config))
    ///     {
    ///         var inputTopic = driver.CreateInputTopic&lt;string, string&gt;("test");
    ///         var outputTopic = driver.CreateOuputTopic&lt;string, string&gt;("test-output", TimeSpan.FromSeconds(5));
    ///         inputTopic.PipeInput("test", "test-1234");
    ///         var r = outputTopic.ReadKeyValue();
    ///         // YOU SOULD ASSERT HERE
    ///     }
    /// }
    /// </code>
    /// </example>
    /// </summary>
    public class TopologyTestDriver : IDisposable
    {
        private readonly CancellationTokenSource tokenSource = new CancellationTokenSource();
        private readonly InternalTopologyBuilder topologyBuilder;
        private readonly IStreamConfig configuration;
        private readonly IStreamConfig topicConfiguration;
        private readonly ProcessorTopology processorTopology;

        private readonly IDictionary<string, IPipeInput> inputs = new Dictionary<string, IPipeInput>();
        private readonly IDictionary<string, IPipeOutput> outputs = new Dictionary<string, IPipeOutput>();
        private readonly PipeBuilder pipeBuilder = null;

        private readonly IThread threadTopology = null;
        private readonly IKafkaSupplier kafkaSupplier = null;

        /// <summary>
        /// Create a new test diver instance.
        /// </summary>
        /// <param name="topology">Topology to be tested</param>
        /// <param name="config">Configuration for topology. One property will be modified : <see cref="IStreamConfig.NumStreamThreads"/> will set to 1</param>
        public TopologyTestDriver(Topology topology, IStreamConfig config)
            :this(topology.Builder, config)
        { }

        private TopologyTestDriver(InternalTopologyBuilder builder, IStreamConfig config)
        {
            this.topologyBuilder = builder;
            this.configuration = config;

            // ONLY 1 thread for test driver
            this.configuration.NumStreamThreads = 1;
            this.configuration.Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;

            this.topicConfiguration = config.Clone();
            this.topicConfiguration.ApplicationId = $"test-driver-{this.configuration.ApplicationId}";

            var processID = Guid.NewGuid();
            var clientId = string.IsNullOrEmpty(configuration.ClientId) ? $"{this.configuration.ApplicationId.ToLower()}-{processID}" : configuration.ClientId;
            this.configuration.ClientId = clientId;

            kafkaSupplier = new MockKafkaSupplier();
            pipeBuilder = new PipeBuilder(kafkaSupplier);

            this.processorTopology = this.topologyBuilder.BuildTopology();

            this.threadTopology = StreamThread.Create(
                $"{this.configuration.ApplicationId.ToLower()}-stream-thread-0",
                clientId,
                builder,
                config,
                kafkaSupplier,
                kafkaSupplier.GetAdmin(configuration.ToAdminConfig($"{clientId}-admin")),
                0);

            RunDriver();
        }

        private void RunDriver()
        {
            bool isRunningState = false;
            DateTime dt = DateTime.Now;
            TimeSpan timeout = TimeSpan.FromSeconds(30);

            threadTopology.StateChanged += (thread, old, @new) => {
                if (@new is Processors.ThreadState && ((Processors.ThreadState)@new) == Processors.ThreadState.RUNNING)
                    isRunningState = true;
            };

            threadTopology.Start(tokenSource.Token);
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                    throw new StreamsException($"Test topology driver can't initiliaze state after {timeout.TotalSeconds} seconds !");
            }
        }

        /// <summary>
        /// Close the driver, its topology, and all processors.
        /// </summary>
        public void Dispose()
        {
            tokenSource.Cancel();
            threadTopology.Dispose();

            foreach (var k in inputs)
                k.Value.Dispose();

            foreach (var k in outputs)
                k.Value.Dispose();
        }

        #region Create Input Topic

        /// <summary>
        /// Create <see cref="TestInputTopic{K, V}"/> to be used for piping records to topic.
        /// The key and value serializer as specified in the <see cref="IStreamConfig"/> are used.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="topicName">the name of the topic</param>
        /// <returns><see cref="TestInputTopic{K, V}"/> instance</returns>
        public TestInputTopic<K,V> CreateInputTopic<K, V>(string topicName)
            => CreateInputTopic<K, V>(topicName, null, null);

        /// <summary>
        /// Create <see cref="TestInputTopic{K, V}"/> to be used for piping records to topic
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="keySerdes">Key serializer</param>
        /// <param name="valueSerdes">Value serializer</param>
        /// <param name="topicName">the name of the topic</param>
        /// <returns><see cref="TestInputTopic{K, V}"/> instance</returns>
        public TestInputTopic<K,V> CreateInputTopic<K, V>(string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            var pipe = pipeBuilder.Input(topicName, this.topicConfiguration);
            inputs.Add(topicName, pipe);
            return new TestInputTopic<K, V>(pipe, this.topicConfiguration, keySerdes, valueSerdes);
        }

        /// <summary>
        /// Create <see cref="TestInputTopic{K, V}"/> to be used for piping records to topic
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="KS">Key serializer type</typeparam>
        /// <typeparam name="VS">Value serializer type</typeparam>
        /// <param name="topicName">the name of the topic</param>
        /// <returns><see cref="TestInputTopic{K, V}"/> instance</returns>
        public TestInputTopic<K, V> CreateInputTopic<K, V, KS, VS>(string topicName)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => CreateInputTopic<K, V>(topicName, new KS(), new VS());

        #endregion

        #region Create Output Topic

        /// <summary>
        /// Create <see cref="TestOutputTopic{K, V}"/> to be used for reading records from topic.
        /// The key and value serializer as specified in the <see cref="IStreamConfig"/> are used.
        /// By default, the consume timeout is set to 5 seconds.
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <param name="topicName">the name of the topic</param>
        /// <returns><see cref="TestOutputTopic{K, V}"/> instance</returns>
        public TestOutputTopic<K, V> CreateOuputTopic<K, V>(string topicName)
            => CreateOuputTopic<K, V>(topicName, TimeSpan.FromSeconds(5), null, null);

        /// <summary>
        /// Create <see cref="TestOutputTopic{K, V}"/> to be used for reading records from topic.
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <param name="topicName">the name of the topic</param>
        /// <param name="consumeTimeout">Consumer timeout</param>
        /// <param name="keySerdes">Key deserializer</param>
        /// <param name="valueSerdes">Value deserializer</param>
        /// <returns><see cref="TestOutputTopic{K, V}"/> instance</returns>
        public TestOutputTopic<K, V> CreateOuputTopic<K, V>(string topicName, TimeSpan consumeTimeout, ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null)
        {
            var pipe = pipeBuilder.Output(topicName, consumeTimeout, this.topicConfiguration, this.tokenSource.Token);
            outputs.Add(topicName, pipe);
            return new TestOutputTopic<K, V>(pipe, this.topicConfiguration, keySerdes, valueSerdes);
        }

        /// <summary>
        /// Create <see cref="TestOutputTopic{K, V}"/> to be used for reading records from topic.
        /// By default, the consume timeout is set to 5 seconds.
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <typeparam name="KS">Key serializer type</typeparam>
        /// <typeparam name="VS">Value serializer type</typeparam>
        /// <param name="topicName">the name of the topic</param>
        /// <returns><see cref="TestOutputTopic{K, V}"/> instance</returns>
        public TestOutputTopic<K, V> CreateOuputTopic<K, V, KS, VS>(string topicName)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => CreateOuputTopic<K, V, KS, VS>(topicName, TimeSpan.FromSeconds(5));

        /// <summary>
        /// Create <see cref="TestOutputTopic{K, V}"/> to be used for reading records from topic.
        /// </summary>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <typeparam name="KS">Key serializer type</typeparam>
        /// <typeparam name="VS">Value serializer type</typeparam>
        /// <param name="topicName">the name of the topic</param>
        /// <param name="consumeTimeout">Consumer timeout</param>
        /// <returns><see cref="TestOutputTopic{K, V}"/> instance</returns>
        public TestOutputTopic<K, V> CreateOuputTopic<K, V, KS, VS>(string topicName, TimeSpan consumeTimeout)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => CreateOuputTopic<K, V>(topicName, consumeTimeout, new KS(), new VS());

        #endregion
    }
}
