using Confluent.Kafka;
using kafka_stream_core.Kafka;
using kafka_stream_core.Kafka.Internal;
using kafka_stream_core.Processors;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal;
using System;

namespace kafka_stream_core
{
    public class KafkaStream
    {
        private readonly Topology topology;
        private readonly IStreamConfig configuration;
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly IThread[] threads;
        private readonly ProcessorTopology processorTopology;
        private readonly IAdminClient adminClient;

        public KafkaStream(Topology topology, IStreamConfig configuration)
        {
            this.topology = topology;
            this.configuration = configuration;
            this.kafkaSupplier = new DefaultKafkaClientSupplier();

            // sanity check
            this.processorTopology = topology.Builder.BuildTopology();
            
            this.threads = new IThread[this.configuration.NumStreamThreads];

            for (int i = 0; i < this.configuration.NumStreamThreads; ++i)
            {
                var processID = Guid.NewGuid();
                var clientId = $"{this.configuration.ApplicationId.ToLower()}-{processID}";
                var threadId = $"{this.configuration.ApplicationId.ToLower()}-stream-thread-{i}";

                adminClient = this.kafkaSupplier.GetAdmin(configuration.ToAdminConfig(StreamThread.GetSharedAdminClientId(clientId)));

                this.threads[i] = StreamThread.Create(
                    threadId,
                    clientId,
                    this.topology.Builder,
                    configuration,
                    this.kafkaSupplier,
                    adminClient);
            }
        }

        public void Start()
        {
            foreach (var t in threads)
                t.Start();
        }

        public void Stop()
        {
            foreach (var t in threads)
                t.Dispose();
        }

        public void Kill()
        {
            foreach (var t in threads)
            {
                if (!t.IsDisposable)
                {
                    try
                    {
                        t.Dispose();
                    }catch(Exception e)
                    {
                        // TODO
                    }
                }
            }
        }
    }
}
