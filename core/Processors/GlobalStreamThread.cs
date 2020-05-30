using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

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
                this.globalConsumer.Assign(partitionOffsets.Keys.Select(x => new TopicPartitionOffset(x, Offset.Beginning)));

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

                    DateTime dt = DateTime.Now;
                    if (dt >= this.lastFlush.Add(this.flushInterval))
                    {
                        this.globalStateMaintainer.FlushState();
                        this.lastFlush = DateTime.Now;
                    }
                }
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
        private readonly IStreamConfig configuration;
        private StateConsumer stateConsumer;
        private readonly IGlobalStateMaintainer globalStateMaintainer;

        public GlobalStreamThread(string threadClientId,
            IConsumer<byte[], byte[]> globalConsumer,
            IStreamConfig configuration,
            IGlobalStateMaintainer globalStateMaintainer)
        {
            logPrefix = $"global-stream-thread {threadClientId} ";

            this.globalConsumer = globalConsumer;
            this.configuration = configuration;
            this.globalStateMaintainer = globalStateMaintainer;

            thread = new Thread(Run);
            State = GlobalThreadState.CREATED;
        }

        private void Run()
        {
            SetState(GlobalThreadState.RUNNING);
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

            try
            {
                this.stateConsumer = InitializeStateConsumer();
            }
            catch
            {
                SetState(GlobalThreadState.PENDING_SHUTDOWN);
                SetState(GlobalThreadState.DEAD);

                log.Warn($"{logPrefix}Error happened during initialization of the global state store; this thread has shutdown");

                throw;
            }

            this.token = token;

            thread.Start();
        }

        private StateConsumer InitializeStateConsumer()
        {
            try
            {
                var stateConsumer = new StateConsumer(
                    this.globalConsumer,
                    this.globalStateMaintainer,
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

                try
                {
                    this.stateConsumer.Close();
                }
                catch (Exception e)
                {
                    log.Error($"{logPrefix}exception caught during disposing of GlobalStreamThread.", e);
                    // ignore exception
                    // https://docs.microsoft.com/en-us/visualstudio/code-quality/ca1065
                }
                
                SetState(GlobalThreadState.DEAD);
                log.Info($"{logPrefix}Shutdown complete");

                this.disposed = true;
            }
        }

        #endregion
    }
}
