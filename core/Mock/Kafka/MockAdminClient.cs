using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Streamiz.Kafka.Net.Mock.Sync;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal sealed class MockAdminClient : SyncAdminClient
    {
        public MockAdminClient(MockCluster cluster, string name)
        {
            Name = name;
        }
    }
}
