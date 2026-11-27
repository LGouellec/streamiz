using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices.ComTypes;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Mock.Sync
{
    internal class SyncPipeBuilder : IPipeBuilder
    {
        private class StreamTaskPublisher : ISyncPublisher
        {
            private readonly StreamTask task;

            public StreamTaskPublisher(StreamTask task)
            {
                this.task = task;
            }

            // next offset by partition
            private readonly Dictionary<TopicPartition, long> offsets = new();

            public void PublishRecord(string topic, int partition, byte[] key, byte[] value, DateTime timestamp, Headers headers)
            {
                var topicPartition = new TopicPartition(topic, partition);
                offsets.TryGetValue(topicPartition, out var offset);
                offsets[topicPartition] = offset + 1;

                task.AddRecord(new ConsumeResult<byte[], byte[]>
                {
                    Topic = topic,
                    TopicPartitionOffset = new TopicPartitionOffset(topicPartition, offset),
                    Message = new Message<byte[], byte[]> { Key = key, Value = value, Timestamp = new Timestamp(timestamp), Headers = headers }
                });
            }

            public void Flush()
            {
                // Use the task's wall clock time which is mock-aware
                long now = task.WallClockTime;
                TaskManager.CurrentTask = task;
                while (task.CanProcess(now))
                    task.Process();

                task.PunctuateStreamTime();
                task.PunctuateSystemTime();
                TaskManager.CurrentTask = null;
            }

            public void Close()
            {
            }
        }

        private class GlobalTaskPublisher : ISyncPublisher
        {
            private readonly GlobalStateUpdateTask globalTask;
            private int offset = 0;
            
            public GlobalTaskPublisher(GlobalStateUpdateTask globalTask)
            {
                this.globalTask = globalTask;
            }

            public void PublishRecord(string topic, int partition, byte[] key, byte[] value, DateTime timestamp, Headers headers)
            {
                // a global task reads all partitions of its topic: records are kept on partition 0
                globalTask.Update(new ConsumeResult<byte[], byte[]>
                {
                    Topic = topic,
                    TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition(topic, 0), offset++),
                    Message = new Message<byte[], byte[]> { Key = key, Value = value, Timestamp = new Timestamp(timestamp), Headers = headers }
                });
            }

            public void Flush()
            {
                globalTask.FlushState();
            }

            public void Close()
            {
                
            }
        }
        
        private class ExternalTaskPublisher : ISyncPublisher
        {
            private readonly ExternalProcessorTopologyExecutor externalProcessorTopologyExecutor;
            private int offset = 0;
            public ExternalTaskPublisher(ExternalProcessorTopologyExecutor externalProcessorTopologyExecutor)
            {
                this.externalProcessorTopologyExecutor = externalProcessorTopologyExecutor;
            }
            
            public void PublishRecord(string topic, int partition, byte[] key, byte[] value, DateTime timestamp, Headers headers)
            {
                var result = new ConsumeResult<byte[], byte[]>
                {
                    Topic = topic,
                    TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition(topic, 0), offset++),
                    Message = new Message<byte[], byte[]>
                        {Key = key, Value = value, Timestamp = new Timestamp(timestamp), Headers = headers}
                };
                externalProcessorTopologyExecutor.Process(result);
            }

            public void Flush()
            {
                externalProcessorTopologyExecutor.Flush();
            }

            public void Close()
            {
                
            }
        }
        
        private readonly ISyncPublisher publisher;
        private readonly SyncProducer mockProducer;

        // used to route piped records to a partition, like the producer would do
        private readonly SyncProducer partitionsSource;

        public SyncPipeBuilder(StreamTask task, SyncProducer partitionsSource = null)
        {
            publisher = new StreamTaskPublisher(task);
            this.partitionsSource = partitionsSource;
        }
        
        public SyncPipeBuilder(GlobalStateUpdateTask globalTask)
        {
            publisher = new GlobalTaskPublisher(globalTask);
        }

        public SyncPipeBuilder(SyncProducer mockProducer)
        {
            this.mockProducer = mockProducer;
        }

        public SyncPipeBuilder(ExternalProcessorTopologyExecutor externalProcessorTopology)
        {
            publisher = new ExternalTaskPublisher(externalProcessorTopology);
        }

        public IPipeInput Input(string topic, IStreamConfig configuration)
        {
            return new SyncPipeInput(publisher, topic, partitionsSource, configuration?.ToProducerConfig().Partitioner);
        }

        public IPipeOutput Output(string topic, TimeSpan consumeTimeout, IStreamConfig configuration, CancellationToken token = default)
        {
            return new SyncPipeOutput(topic, consumeTimeout, configuration, mockProducer, token);
        }
    }
}