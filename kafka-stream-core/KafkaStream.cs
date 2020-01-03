using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core
{
    public class KafkaStream
    {
        private readonly Topology topology;
        private readonly Configuration configuration;
        private ContextOperator context;

        public KafkaStream(Topology topology, Configuration configuration)
        {
            this.topology = topology;
            this.configuration = configuration;
        }

        public void start()
        {
            context = new ContextOperator(configuration);

            topology.OperatorChain.Init(context);
            topology.OperatorChain.Start();
        }

        public void stop()
        {
            topology.OperatorChain.Stop();
            context.Client.Dispose();
        }

        public void kill()
        {
            topology.OperatorChain.Kill();
            context.Client.Dispose();
        }
    }
}
