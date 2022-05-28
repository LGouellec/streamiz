using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Kafka.Net.Processors
{
    internal class ExternalStreamThread : IThread
    {
        private static readonly ILogger log = Logger.GetLogger(typeof(ExternalStreamThread));
        private readonly string clientId;
        private readonly IConsumer<byte[], byte[]> consumer;
        private readonly IProducer<byte[], byte[]> producer;
        private readonly IEnumerable<string> externalSourceTopics;
        private readonly IDictionary<string, ExternalProcessorTopology> externalProcessorTopologies;
        private readonly StreamMetricsRegistry streamMetricsRegistry;
        private readonly IStreamConfig configuration;
        private readonly string logPrefix;
        private readonly Thread thread;
        private CancellationToken token;

        public ExternalStreamThread(
            string threadId,
            string clientId,
            IConsumer<byte[], byte[]> consumer,
            IProducer<byte[], byte[]> producer,
            IEnumerable<string> externalSourceTopics,
            IDictionary<string, ExternalProcessorTopology> externalProcessorTopologies,
            StreamMetricsRegistry streamMetricsRegistry,
            IStreamConfig configuration)
        {
            this.clientId = clientId;
            this.consumer = consumer;
            this.producer = producer;
            this.externalSourceTopics = externalSourceTopics;
            this.externalProcessorTopologies = externalProcessorTopologies;
            this.streamMetricsRegistry = streamMetricsRegistry;
            this.configuration = configuration;
            
            thread = new Thread(Run);
            thread.Name = threadId;
            Name = threadId;
            logPrefix = $"external-stream-thread[{threadId}] ";
            
            State = ThreadState.CREATED;
            
            // Todo : sensors
        }
        
        public void Dispose() => CloseThread();

        public int Id => thread.ManagedThreadId;
        public ThreadState State { get; private set; }
        
        public bool IsDisposable { get; private set; } = false;
        public string Name { get; }
        
        public bool IsRunning { get; private set; } = false;
        
        public void Run()
        {
            try
            {
                SetState(ThreadState.RUNNING);
                while (!token.IsCancellationRequested)
                {
                    long now = DateTime.Now.GetMilliseconds();
                    var result = consumer.Consume();
                    if (result != null)
                    {
                        // do external call
                        // throw NotEnoughTimeException
                        // consume and put in local buffer, stop assign partitions if buffer size is full
                        // and continue to processing
                    }

                    Commit();
                }

            }
            finally
            {
                CompleteShutdown();
            }
            
        }

        private void CompleteShutdown()
        {
            try
            {
                if (!IsDisposable)
                {
                    log.LogInformation($"{logPrefix}Shutting down");

                    SetState(ThreadState.PENDING_SHUTDOWN);

                    IsRunning = false;
                    
                    // clear local buffer
                    
                    consumer.Unsubscribe();
                    consumer.Close();
                    consumer.Dispose();

                  //  streamMetricsRegistry.RemoveThreadSensors(threadId);
                    log.LogInformation($"{logPrefix}Shutdown complete");
                    IsDisposable = true;
                }
            }
            catch (Exception e)
            {
                log.LogError(e,
                    "{LogPrefix}Failed to close external stream thread due to the following error:", logPrefix);
            }
            finally
            {
                SetState(ThreadState.DEAD);
            }        
        }

        private void Commit()
        {
            throw new NotImplementedException();
        }
        
        private void CloseThread()
        {
            try
            {
                thread.Join();
            }
            catch (Exception e)
            {
                log.LogError(e,
                    "{LogPrefix}Failed to close external stream thread due to the following error:", logPrefix);
            }
        }

        public void Start(CancellationToken token)
        {
            log.LogInformation("{LogPrefix}Starting", logPrefix);
            if (SetState(ThreadState.STARTING) == null)
            {
                log.LogInformation($"{logPrefix}StreamThread already shutdown. Not running");
                IsRunning = false;
                return;
            }

            this.token = token;
            IsRunning = true;
            
            // check conf with max poll interval ms + retry backoff * number retry
            
            consumer.Subscribe(externalSourceTopics);
            SetState(ThreadState.PARTITIONS_ASSIGNED);
            thread.Start();     
        }

        internal ThreadState SetState(ThreadState newState)
        {
            if (State.IsValidTransition(newState))
                State = newState;
            else
                throw new StreamsException($"{logPrefix}Unexpected state transition from {State} to {newState}");
            
            return State;
        }

        public IEnumerable<ITask> ActiveTasks => throw new NotSupportedException();

        public event ThreadStateListener StateChanged;
    }
}