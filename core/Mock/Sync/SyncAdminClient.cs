using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncAdminClient : IAdminClient
    {
        public Handle Handle => throw new NotImplementedException();

        public string Name { get; protected set; }

        public int AddBrokers(string brokers)
        {
            throw new NotImplementedException();
        }

        public Task AlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, AlterConfigsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task CreatePartitionsAsync(IEnumerable<PartitionsSpecification> partitionsSpecifications, CreatePartitionsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<List<DeleteRecordsResult>> DeleteRecordsAsync(IEnumerable<TopicPartitionOffset> topicPartitionOffsets, DeleteRecordsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<List<DescribeConfigsResult>> DescribeConfigsAsync(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public Metadata GetMetadata(string topic, TimeSpan timeout)
        {
            var error = new Error(ErrorCode.NoError);

            var brokersMetadata = new List<BrokerMetadata> {
                new BrokerMetadata(1, "localhost", 9092)
            };

            var partitionsMetadata = new List<PartitionMetadata>
            {
                new PartitionMetadata(1, 1, new int[1]{1}, new int[1]{1}, error)
            };

            var topicMetadata = new TopicMetadata(topic, partitionsMetadata, error);

            return new Metadata(brokersMetadata,
                new List<TopicMetadata>() { topicMetadata },
                1, "localhost");
        }

        public Metadata GetMetadata(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public GroupInfo ListGroup(string group, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public List<GroupInfo> ListGroups(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }
    }
}
