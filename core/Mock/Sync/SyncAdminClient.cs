using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncAdminClient : IAdminClient
    {
        private readonly SyncProducer producer;
        private AdminClientConfig config;

        public SyncAdminClient(SyncProducer producer)
        {
            this.producer = producer;
        }

        internal void UseConfig(AdminClientConfig config)
        {
            this.config = config;
        }

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

        public Task DeleteGroupsAsync(IList<string> groups, DeleteGroupsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null)
        {
            return Task.Run(() =>
            {
                foreach (var t in topics)
                    producer.CreateTopic(t.Name);
            });
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

        public Task DeleteTopicsAsync(IEnumerable<string> topics, DeleteTopicsOptions options = null)
        {
            throw new NotImplementedException();
        }

        public Task<List<DescribeConfigsResult>> DescribeConfigsAsync(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null)
        {
            return Task.FromResult(new List<DescribeConfigsResult>
            {
                new DescribeConfigsResult
                {
                    Entries = new Dictionary<string, ConfigEntryResult>
                    {
                        {"num.partitions", new ConfigEntryResult{
                                IsDefault = true,
                                IsReadOnly = true,
                                IsSensitive = false,
                                Name = "num.partitions",
                                Value = "1",
                                Source = ConfigSource.DefaultConfig
                            }
                        }
                    }
                }
            });
        }

        public void Dispose()
        {
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
            var topicsMetadata = new List<TopicMetadata>();
            var topics = producer.GetAllTopics();
            var error = new Error(ErrorCode.NoError);

            var brokersMetadata = new List<BrokerMetadata> {
                new BrokerMetadata(1, "localhost", 9092)
            };

            foreach(var t in topics)
            {
                var partitionsMetadata = new List<PartitionMetadata> {
                    new PartitionMetadata(0, 0, new int[1]{0}, new int[1]{0}, error)
                };

                topicsMetadata.Add(new TopicMetadata(t, partitionsMetadata, error));
            }

            return new Metadata(brokersMetadata, topicsMetadata, 1, "localhost");
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
