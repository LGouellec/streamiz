using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Streamiz.Kafka.Net.Processors
{
    internal class GlobalStreamThread : IDisposable
    {
        private class StateConsumer
        {
            private readonly IConsumer<byte[], byte[]> globalConsumer;
            private readonly ILog log = Logger.GetLogger(typeof(StateConsumer));
            private readonly IGlobalStateMaintainer globalStateMaintainer;
            private readonly TimeSpan pollTime;
            private readonly TimeSpan flushInterval;
            private DateTime lastFlush;

            public StateConsumer(
                IConsumer<byte[], byte[]> globalConsumer,
                IGlobalStateMaintainer globalStateMaintainer,
                TimeSpan pollTime,
                TimeSpan flushInterval)
            {
                this.globalConsumer = globalConsumer;
                this.globalStateMaintainer = globalStateMaintainer;
                this.pollTime = pollTime;
                this.flushInterval = flushInterval;
            }

            public void Initialize()
            {
                IDictionary<TopicPartition, long> partitionOffsets = this.globalStateMaintainer.Initialize();
                this.globalConsumer.Assign(partitionOffsets.Keys);

                // TODO: if we don't wait seek will throw. Why is that? How to solve it?
                Thread.Sleep(5000);
                foreach (var entry in partitionOffsets)
                {
                    this.globalConsumer.Seek(new TopicPartitionOffset(entry.Key, entry.Value));
                }
                this.lastFlush = DateTime.Now;
            }

            public void PollAndUpdate()
            {
                try
                {
                    var received = this.globalConsumer.ConsumeRecords(this.pollTime);
                    foreach (var record in received)
                    {
                        this.globalStateMaintainer.Update(record);
                    }

                    // TODO: we might want to provide a wrapper around DateTime so that we can unit test this
                    DateTime dt = DateTime.Now;
                    if (dt >= this.lastFlush.Add(this.flushInterval))
                    {
                        this.globalStateMaintainer.FlushState();
                        this.lastFlush = DateTime.Now;
                    }
                }
                // TODO: should we catch all exceptions?
                catch (Exception e)
                {
                    log.Error("Updating global state failed.", e);
                    throw new StreamsException("Updating global state failed.", e);
                }
            }

            public void Close()
            {
                try
                {
                    this.globalConsumer.Close();
                }
                catch (Exception e)
                {
                    // just log an error if the consumer throws an exception during close
                    // so we can always attempt to close the state stores.
                    log.Error("Failed to close global consumer due to the following error:", e);
                }

                this.globalStateMaintainer.Close();
            }
        }

        public GlobalThreadState State { get; private set; }

        public event GlobalThreadStateListener StateChanged;

        private readonly ILog log = Logger.GetLogger(typeof(GlobalStreamThread));
        private readonly Thread thread;
        private readonly string logPrefix;
        private readonly IConsumer<byte[], byte[]> globalConsumer;
        private CancellationToken token;
        private readonly object stateLock = new object();
        private IAdminClient adminClient;
        private readonly IStreamConfig configuration;
        private StateConsumer stateConsumer;
        private ProcessorTopology topology;

        public GlobalStreamThread(ProcessorTopology topology,
            string threadClientId,
            IConsumer<byte[], byte[]> globalConsumer,
            IStreamConfig configuration,
            IAdminClient adminClient)
        {
            logPrefix = $"global-stream-thread {threadClientId} ";

            this.globalConsumer = globalConsumer;
            this.configuration = configuration;
            this.topology = topology;
            this.adminClient = adminClient;

            thread = new Thread(Run);
            State = GlobalThreadState.CREATED;
        }

        private void Run()
        {
            try
            {
                while (!token.IsCancellationRequested && this.State.IsRunning())
                {
                    this.stateConsumer.PollAndUpdate();
                }
            }
            finally
            {
                Dispose(true, false);
            }
        }

        public void Start(CancellationToken token)
        {
            log.Info($"{logPrefix}Starting");

            this.stateConsumer = InitializeStateConsumer();
            if (this.stateConsumer == null)
            {
                SetState(GlobalThreadState.PENDING_SHUTDOWN);
                SetState(GlobalThreadState.DEAD);

                log.Warn($"{logPrefix}Error happened during initialization of the global state store; this thread has shutdown");

                // TODO: should we throw something here?
                return;
            }

            SetState(GlobalThreadState.RUNNING);
            this.token = token;

            thread.Start();
        }

        private StateConsumer InitializeStateConsumer()
        {
            try
            {
                var stateManager = new GlobalStateManager(this.topology, this.adminClient);
                var context = new ProcessorContext(this.configuration, stateManager);
                stateManager.SetGlobalProcessorContext(context);
                var globalStateUpdateTask = new GlobalStateUpdateTask(stateManager, this.topology, context);
                var stateConsumer = new StateConsumer(
                    this.globalConsumer,
                    globalStateUpdateTask,
                    // if poll time is bigger than int allows something is probably wrong anyway
                    new TimeSpan(0, 0, 0, 0, (int)this.configuration.PollMs),
                    new TimeSpan(0, 0, 0, 0, (int)this.configuration.CommitIntervalMs));
                stateConsumer.Initialize();
                return stateConsumer;
            }
            catch(StreamsException)
            {
                throw;
            }
            catch(Exception e)
            {
                throw new StreamsException("Exception caught during initialization of GlobalStreamThread", e);
            }
        }

        private void SetState(GlobalThreadState newState)
        {
            GlobalThreadState oldState;

            lock (stateLock)
            {
                oldState = this.State;

                if (oldState == GlobalThreadState.PENDING_SHUTDOWN && newState == GlobalThreadState.PENDING_SHUTDOWN)
                {
                    // when the state is already in PENDING_SHUTDOWN, its transition to itself
                    // will be refused but we do not throw exception here
                    return;
                }
                else if (oldState == GlobalThreadState.DEAD)
                {
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return;
                }
                else if (!oldState.IsValidTransition(newState))
                {
                    log.Error($"{logPrefix}Unexpected state transition from {oldState} to {newState}");
                    throw new StreamsException($"Unexpected state transition from {oldState} to {newState}");
                }
                else
                {
                    log.Info($"{logPrefix}State transition from {oldState} to {newState}");
                }

                State = newState;
            }

            StateChanged?.Invoke(this, oldState, State);
        }

        #region IDisposable

        private bool disposed = false;

        public void Dispose()
        {
            Dispose(true, true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing, bool waitForThread)
        {
            if (!this.disposed)
            {
                // we don't have any unmanaged resources to dispose of so we can ignore value of `disposing`

                SetState(GlobalThreadState.PENDING_SHUTDOWN);
                log.Info($"{logPrefix}Shutting down");

                if (waitForThread)
                {
                    thread.Join();
                }

                //TODO: can this throw? should we try/catch?
                this.stateConsumer.Close();

                SetState(GlobalThreadState.DEAD);
                log.Info($"{logPrefix}Shutdown complete");

                this.disposed = true;
            }
        }

        #endregion
    }
}
