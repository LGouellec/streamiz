using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal delegate void ExceptionOnAssignment(Exception e, IEnumerable<TopicPartition> partitions);

    internal class StreamsRebalanceListener : IConsumerRebalanceListener
    {
        private readonly ILogger log = Logger.GetLogger(typeof(StreamsRebalanceListener));
        private readonly TaskManager manager;

        public event ExceptionOnAssignment ExceptionOnAssignment;

        internal StreamThread Thread { get; set; }

        public StreamsRebalanceListener(TaskManager manager)
        {
            this.manager = manager;
        }

        public void PartitionsAssigned(IConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions)
        {
            try
            {
                log.LogInformation($"New partitions assign requested : {string.Join(",", partitions)}");
                
                DateTime start = DateTime.Now;
                manager.RebalanceInProgress = true;
                manager.CreateTasks(partitions);
                Thread.SetState(ThreadState.PARTITIONS_ASSIGNED);
                Thread.LastPartitionAssignedTime = start.GetMilliseconds();
                manager.RebalanceInProgress = false;

                StringBuilder sb = new StringBuilder();
                sb.AppendLine($"Partition assignment took {DateTime.Now - start} ms.");
                sb.AppendLine($"\tCurrently assigned active tasks: {string.Join(",", this.manager.ActiveTaskIds)}");
                log.LogInformation(sb.ToString());
            }
            catch (Exception e)
            {
                ExceptionOnAssignment?.Invoke(e, partitions);
            }
        }

        public void PartitionsRevoked(IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> partitions)
        {
            DateTime start = DateTime.Now;
            lock (manager._lock)
            {
                if (Thread.IsRunning)
                {
                    manager.RebalanceInProgress = true;
                    manager.RevokeTasks(new List<TopicPartition>(partitions.Select(p => p.TopicPartition)));
                    Thread.SetState(ThreadState.PARTITIONS_REVOKED);
                    manager.RebalanceInProgress = false;

                    StringBuilder sb = new StringBuilder();
                    sb.AppendLine($"Partition revocation took {DateTime.Now - start} ms");
                    sb.AppendLine(
                        $"\tCurrent suspended active tasks: {string.Join(",", partitions.Select(p => $"{p.Topic}-{p.Partition}"))}");
                    log.LogInformation(sb.ToString());
                }
            }
        }

        public void PartitionsLost(IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> partitions)
        {
            DateTime start = DateTime.Now;
            try
            {
                manager.RebalanceInProgress = true;
                manager.HandleLostAll();
            }
            finally
            {
                manager.RebalanceInProgress = false;
                log.LogInformation($"Partitions lost took {DateTime.Now - start} ms");
            }
        }
    }
}