using kafka_stream_core.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace kafka_stream_core.Mock.Kafka
{
    internal class MockConsumerInformation
    {
        public string Name { get; set; }
        public MockConsumer Customer { get; set; }
        public IConsumerRebalanceListener RebalanceListener { get; set; }
    }

    internal class MockCluster
    {
        #region Singleton

        private static object _lock = new object();
        private static MockCluster cluster = null;

        private MockCluster() { }

        public MockCluster Instance
        {
            get
            {
                lock (_lock)
                {
                    if (cluster == null)
                        cluster = new MockCluster();
                }
                return cluster;
            }
        }

        #endregion

        private readonly IList<MockTopic> topics = new List<MockTopic>();
        private readonly IDictionary<string, IList<MockConsumerInformation>> consumers = new Dictionary<string, IList<MockConsumerInformation>>();
        private readonly IDictionary<string, int> consumersOffsets = new Dictionary<string, int>();

        #region Topic Gesture

        private bool CreateTopic(string topic, int partitions)
        {
            if (!topics.Any(t => t.Name.Equals(topic, StringComparison.InvariantCultureIgnoreCase)))
            {
                var t = new MockTopic();
                topics.Add(t);
                return true;
            }
            return false;
        }

        #endregion

        #region Partitions Gesture

        #endregion

        #region Consumer Gesture

        #endregion
    }
}
