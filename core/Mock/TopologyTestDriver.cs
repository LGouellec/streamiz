using Confluent.Kafka;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Mock
{
    // TODO : 
    // test.mock.num.brokers = 3
    public class TopologyTestDriver
    {
        private readonly InternalTopologyBuilder topologyBuilder;
        private readonly IStreamConfig configuration;
        private readonly ProcessorTopology processorTopology;

        public TopologyTestDriver(Topology topology, IStreamConfig config)
            :this(topology.Builder, config)
        { }

        // https://github.com/confluentinc/confluent-kafka-dotnet/blob/1.4.x/test/Confluent.Kafka.UnitTests/MoqExample.cs

        private TopologyTestDriver(InternalTopologyBuilder builder, IStreamConfig config)
        {
            this.topologyBuilder = builder;
            this.configuration = config;
            this.configuration.Update(StreamConfig.numStreamThreadsCst, 1);


            this.processorTopology = this.topologyBuilder.BuildTopology();
        }
    }
}
