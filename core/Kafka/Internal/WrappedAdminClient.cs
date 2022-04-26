/*using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class WrappedAdminClient : IAdminClient
    {
        private readonly IAdminClient internalAdminClient;
        private readonly KafkaLoggerAdapter loggerAdapter;

        public WrappedAdminClient(
            IAdminClient internalAdminClient, 
            KafkaLoggerAdapter loggerAdapter)
        {
            this.internalAdminClient = internalAdminClient;
            this.loggerAdapter = loggerAdapter;
        }
        
        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public int AddBrokers(string brokers)
        {
            throw new NotImplementedException();
        }

        public Handle Handle { get; }
        public string Name { get; }
        public List<GroupInfo> ListGroups(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public GroupInfo ListGroup(string @group, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Metadata GetMetadata(string topic, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Metadata GetMetadata(TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task CreatePartitionsAsync(IEnumerable<PartitionsSpecification> partitionsSpecifications, CreatePartitionsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task AlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, AlterConfigsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<List<DescribeConfigsResult>> DescribeConfigsAsync(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<List<DeleteRecordsResult>> DeleteRecordsAsync(IEnumerable<TopicPartitionOffset> topicPartitionOffsets, DeleteRecordsOptions options = null)
        {
            throw new NotImplementedException();
        }
    }
}*/