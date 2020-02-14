using kafka_stream_core.Kafka;
using kafka_stream_core.Kafka.Internal;
using kafka_stream_core.Stream;

namespace kafka_stream_core
{
    public class KafkaStream
    {
        private readonly Topology topology;
        private readonly Configuration configuration;
        private ProcessorContext context;
        private IKafkaSupplier kafkaSupplier;
        private IKafkaClient kafkaClient;

        public KafkaStream(Topology topology, Configuration configuration)
        {
            this.topology = topology;
            this.configuration = configuration;
            this.kafkaSupplier = new DefaultKafkaClientSupplier();
            this.kafkaClient = new KafkaImplementation(this.configuration, this.kafkaSupplier);
        }

        public void start()
        {
            context = new ProcessorContext(configuration);

            //topology.OperatorChain.Init(context);
            //topology.OperatorChain.Start();
        }

        public void stop()
        {
            //topology.OperatorChain.Stop();
            context.Client.Dispose();
        }

        public void kill()
        {
            //topology.OperatorChain.Kill();
            context.Client.Dispose();
        }
    }
}
