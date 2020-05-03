using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Streamiz.Kafka.Net.Processors
{
    internal class GlobalStreamThread : IDisposable
    {
        private class StateConsumer
        {
            public void Initialize()
            {

            }

            public void PollAndUpdate()
            {

            }

            public void Close()
            {

            }
        }

        public GlobalThreadState State { get; private set; }

        public event GlobalThreadStateListener StateChanged;

        private readonly ILog log = Logger.GetLogger(typeof(GlobalStreamThread));
        private readonly Thread thread;
        private readonly string logPrefix;
        private CancellationToken token;
        private readonly object stateLock = new object();
        private StateConsumer stateConsumer;

        public GlobalStreamThread(string threadClientId)
        {
            logPrefix = $"global-stream-thread {threadClientId} ";

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
                return new StateConsumer();
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

                //TODO: can this throw? should wy try/catch?
                this.stateConsumer.Close();

                SetState(GlobalThreadState.DEAD);
                log.Info($"{logPrefix}Shutdown complete");

                this.disposed = true;
            }
        }

        #endregion
    }
}
