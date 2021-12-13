using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal sealed class MockAdminClient : IAdminClient
    {
        private readonly MockCluster cluster;

        public MockAdminClient(MockCluster cluster, string name) 
        {
            Name = name;
            this.cluster = cluster;
        }

        public Handle Handle => throw new NotImplementedException();

        public string Name { get; }

        public int AddBrokers(string brokers)
        {
            return 1;
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
            return Task.Run(() =>
            {
                foreach (var specs in topics)
                {
                    cluster.CreateTopic(specs.Name, specs.NumPartitions);
                }
            });
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
                                Value = cluster.DEFAULT_NUMBER_PARTITIONS.ToString(),
                                Source = ConfigSource.DefaultConfig
                            }
                        },
                        {"replication.factor", new ConfigEntryResult{
                                IsDefault = true,
                                IsReadOnly = true,
                                IsSensitive = false,
                                Name = "replication.factor",
                                Value = "1",
                                Source = ConfigSource.DefaultConfig
                            }
                        },
                    }
                }
            });
        }

        public void Dispose()
        {
            
        }

        public Metadata GetMetadata(string topic, TimeSpan timeout)
        {
            var metadata = cluster.GetClusterMetadata();
            return new Metadata(
                metadata.Brokers,
                metadata.Topics.FirstOrDefault(t => t.Topic.Equals(topic)).ToSingle().ToList(),
                metadata.OriginatingBrokerId,
                metadata.OriginatingBrokerName);
        }

        public Metadata GetMetadata(TimeSpan timeout)
            => cluster.GetClusterMetadata();

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