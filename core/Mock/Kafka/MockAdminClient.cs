using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal sealed class MockAdminClient : BasedAdminClient
    {
        private readonly MockCluster cluster;

        public MockAdminClient(MockCluster cluster, string name) 
        {
            Name = name;
            this.cluster = cluster;
        }

        public override Handle Handle => null;

        public override string Name { get; }
        

        public override Task CreateTopicsAsync(IEnumerable<TopicSpecification> topics, CreateTopicsOptions options = null)
        {
            return Task.Run(() =>
            {
                foreach (var specs in topics)
                {
                    if(specs.NumPartitions > 0)
                        cluster.CreateTopic(specs.Name, specs.NumPartitions);
                    else
                        cluster.CreateTopic(specs.Name);
                }
            });
        }

        public new Task<List<DeleteRecordsResult>> DeleteRecordsAsync(IEnumerable<TopicPartitionOffset> topicPartitionOffsets, DeleteRecordsOptions options = null)
        {
            var result = cluster.DeleteRecords(topicPartitionOffsets);
            return Task.FromResult(result);
        }
        
        public override Task<List<DescribeConfigsResult>> DescribeConfigsAsync(IEnumerable<ConfigResource> resources, DescribeConfigsOptions options = null)
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

        public override void Dispose()
        {
            
        }

        public override Metadata GetMetadata(string topic, TimeSpan timeout)
        {
            var metadata = cluster.GetClusterMetadata();
            return new Metadata(
                metadata.Brokers,
                metadata.Topics.FirstOrDefault(t => t.Topic.Equals(topic)).ToSingle().ToList(),
                metadata.OriginatingBrokerId,
                metadata.OriginatingBrokerName);
        }

        public override Metadata GetMetadata(TimeSpan timeout)
            => cluster.GetClusterMetadata();
    }
}