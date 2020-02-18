using kafka_stream_core.Kafka;
using kafka_stream_core.Kafka.Internal;
using kafka_stream_core.Processors;
using kafka_stream_core.Stream;
using System;

namespace kafka_stream_core
{
    public class KafkaStream
    {
        private readonly Topology topology;
        private readonly StreamConfig configuration;
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly IThread[] threads;
        private readonly ProcessorTopology processorTopology;

        public KafkaStream(Topology topology, StreamConfig configuration)
        {
            this.topology = topology;
            this.configuration = configuration;
            this.kafkaSupplier = new DefaultKafkaClientSupplier();
            this.processorTopology = this.topology.Builder.buildTopology();

            this.threads = new IThread[this.processorTopology.NumberStreamThreads];

            for (int i = 0; i < this.processorTopology.NumberStreamThreads; ++i)
            {
                var consumer = this.kafkaSupplier.GetConsumer(configuration.toConsumerConfig());
                var producer = this.kafkaSupplier.GetProducer(configuration.toProducerConfig());
                //var context = new ProcessorContext(configuration, consumer, producer);
                var context = new ProcessorContext(configuration);
                var processor = processorTopology.GetSourceProcessor(processorTopology.SourceProcessorNames[i]);

                this.threads[i] = StreamThread.create(
                    $"{this.configuration.ApplicationId.ToLower()}-stream-thread-{i}",
                    consumer,
                    context,
                    processor);
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
