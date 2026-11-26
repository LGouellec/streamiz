using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using Microsoft.Extensions.Logging;

namespace Streamiz.Kafka.Net.Processors
{
    internal class StreamStateManager
    {
        private readonly ILogger log = Logger.GetLogger(typeof(StreamStateManager));
        private readonly Dictionary<long, ThreadState> threadState;
        private readonly HashSet<long> failedThreads = new HashSet<long>();
        private readonly KafkaStream stream;
        private GlobalThreadState globalThreadState;
        private static readonly object threadStatesLock = new object();

        public StreamStateManager(KafkaStream stream, Dictionary<long, ThreadState> threadState, GlobalThreadState globalThreadState)
        {
            this.globalThreadState = globalThreadState;
            this.threadState = threadState;
            this.stream = stream;
        }

        private void MaybeSetError()
        {
            // check if we have at least one thread running
            // a thread shutting down due to a failure is considered as dead : its shutdown can be long (or blocked),
            // and it will never process records again.
            foreach (var kv in threadState)
            {
                if (kv.Value != ThreadState.DEAD &&
                    !(kv.Value == ThreadState.PENDING_SHUTDOWN && failedThreads.Contains(kv.Key)))
                {
                    return;
                }
            }

            if (stream.SetState(KafkaStream.State.ERROR))
            {
                log.LogError("All stream threads have died or are shutting down due to a failure. The instance will be in error state and should be closed");
            }
        }

        /**
         * If all threads are up, including the global thread, set to RUNNING
         */
        private void MaybeSetRunning()
        {
            // state can be transferred to RUNNING if all threads are either RUNNING or DEAD
            foreach ( var state in threadState.Values)
            {
                if (state != ThreadState.RUNNING && state != ThreadState.DEAD)
                {
                    return;
                }
            }

            // the global state thread is relevant only if it is started. There are cases
            // when we don't have a global state thread at all, e.g., when we don't have global KTables
            if (globalThreadState != null && globalThreadState != GlobalThreadState.RUNNING)
            {
                return;
            }

            stream.SetState(KafkaStream.State.RUNNING);
        }


        internal void OnChange(IThread thread, ThreadStateTransitionValidator old, ThreadStateTransitionValidator @new)
        {
            lock (threadStatesLock)
            {
                if (thread is StreamThread)
                {
                    ThreadState newState = (ThreadState)@new;
                    threadState.AddOrUpdate(thread.Id, newState);

                    if (newState == ThreadState.PARTITIONS_REVOKED || newState == ThreadState.PARTITIONS_ASSIGNED)
                    {
                        stream.SetState(KafkaStream.State.REBALANCING);
                    }
                    else if (newState == ThreadState.RUNNING)
                    {
                        MaybeSetRunning();
                    }
                    else if (newState == ThreadState.PENDING_SHUTDOWN)
                    {
                        // propagate the failure to the client as soon as possible, before the (potentially blocking) close of the thread
                        if (thread is StreamThread streamThread && streamThread.IsFailed)
                        {
                            failedThreads.Add(thread.Id);
                            MaybeSetError();
                        }
                    }
                    else if (newState == ThreadState.DEAD)
                    {
                        MaybeSetError();
                    }
                }
            }
        }

        internal void OnGlobalThreadStateChange(GlobalStreamThread thread, ThreadStateTransitionValidator old, ThreadStateTransitionValidator @new)
        {
            lock (threadStatesLock)
            {
                if (thread is GlobalStreamThread)
                {
                    GlobalThreadState newState = (GlobalThreadState)@new;
                    globalThreadState = newState;

                    if (newState == GlobalThreadState.RUNNING)
                    {
                        MaybeSetRunning();
                    }
                    else if (newState == GlobalThreadState.PENDING_SHUTDOWN || newState == GlobalThreadState.DEAD)
                    {
                        // the global thread is only shut down by the client (state already PENDING_SHUTDOWN, so ERROR is refused)
                        // or due to a failure. In the second case, propagate the error as soon as possible.
                        if (stream.SetState(KafkaStream.State.ERROR))
                        {
                            log.LogError("Global thread has died. The instance will be in error state and should be closed");
                        }
                    }
                }
            }
        }
    }
}