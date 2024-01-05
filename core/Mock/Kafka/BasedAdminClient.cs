using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal abstract class BasedAdminClient : IAdminClient
    {
        public abstract void Dispose();

        public void SetSaslCredentials(string username, string password)
        {
            throw new NotImplementedException();
        }

        public abstract Handle Handle { get; }
        public abstract string Name { get; }
        
        public int AddBrokers(string brokers)
        {
            throw new NotImplementedException();
        }

        public Task<List<DeleteRecordsResult>> DeleteRecordsAsync(IEnumerable<TopicPartitionOffset> topicPartitionOffsets, DeleteRecordsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task CreateAclsAsync(IEnumerable<AclBinding> aclBindings, CreateAclsOptions options = null)
        {
            throw new NotImplementedException();
        }
        
        public Task<DescribeAclsResult> DescribeAclsAsync(AclBindingFilter aclBindingFilter, DescribeAclsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<List<DeleteAclsResult>> DeleteAclsAsync(IEnumerable<AclBindingFilter> aclBindingFilters, DeleteAclsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<DeleteConsumerGroupOffsetsResult> DeleteConsumerGroupOffsetsAsync(string @group, IEnumerable<TopicPartition> partitions,
            DeleteConsumerGroupOffsetsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<List<AlterConsumerGroupOffsetsResult>> AlterConsumerGroupOffsetsAsync(IEnumerable<ConsumerGroupTopicPartitionOffsets> groupPartitions, AlterConsumerGroupOffsetsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<List<ListConsumerGroupOffsetsResult>> ListConsumerGroupOffsetsAsync(IEnumerable<ConsumerGroupTopicPartitions> groupPartitions, ListConsumerGroupOffsetsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<ListConsumerGroupsResult> ListConsumerGroupsAsync(ListConsumerGroupsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<DescribeConsumerGroupsResult> DescribeConsumerGroupsAsync(IEnumerable<string> groups, DescribeConsumerGroupsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<DescribeUserScramCredentialsResult> DescribeUserScramCredentialsAsync(IEnumerable<string> users, DescribeUserScramCredentialsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task AlterUserScramCredentialsAsync(IEnumerable<UserScramCredentialAlteration> alterations, AlterUserScramCredentialsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions options = null)
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
        
        public Task AlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, AlterConfigsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<List<IncrementalAlterConfigsResult>> IncrementalAlterConfigsAsync(Dictionary<ConfigResource, List<ConfigEntry>> configs, IncrementalAlterConfigsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task CreatePartitionsAsync(IEnumerable<PartitionsSpecification> partitionsSpecifications, CreatePartitionsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task DeleteGroupsAsync(IList<string> groups, DeleteGroupsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public abstract Metadata GetMetadata(string topic, TimeSpan timeout);
        public abstract Metadata GetMetadata(TimeSpan timeout);

        public abstract Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics,
            CreateTopicsOptions options = null);

        public abstract Task<List<DescribeConfigsResult>> DescribeConfigsAsync(IEnumerable<ConfigResource> resources,
            DescribeConfigsOptions options = null);
    }
}