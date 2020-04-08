using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.Crosscutting;
using kafka_stream_core.Processors.Internal;
using log4net;

namespace kafka_stream_core.Processors
{
    internal class StreamStateManager
    {
        private readonly ILog log = Logger.GetLogger(typeof(StreamStateManager));
        private readonly Dictionary<long, ThreadState> threadState;
        private readonly KafkaStream stream;
        private static readonly object threadStatesLock = new object();

        public StreamStateManager(KafkaStream stream, Dictionary<long, ThreadState> threadState)
        {
            this.threadState = threadState;
            this.stream = stream;
        }

        private void MaybeSetError()
        {
            // check if we have at least one thread running
            foreach (var state in threadState.Values)
            {
                if (state != ThreadState.DEAD)
                {
                    return;
                }
            }

            if (stream.SetState(KafkaStream.State.ERROR))
            {
                log.Error("All stream threads have died. The instance will be in error state and should be closed.");
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
            //if (globalThreadState != null && globalThreadState != GlobalStreamThread.State.RUNNING)
            //{
            //    return;
            //}

            stream.SetState(KafkaStream.State.RUNNING);
        }


        internal void OnChange(IThread thread, ThreadStateTransitionValidator old, ThreadStateTransitionValidator @new)
        {
            lock (threadStatesLock)
            {
                // StreamThreads first
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
                    else if (newState == ThreadState.DEAD)
                    {
                        MaybeSetError();
                    }
                }
                // TODO :
                // else if (thread instanceof GlobalStreamThread) {
                //    // global stream thread has different invariants
                //     GlobalStreamThread.State newState = (GlobalStreamThread.State)abstractNewState;
                //    globalThreadState = newState;

                //    // special case when global thread is dead
                //    if (newState == GlobalStreamThread.State.DEAD)
                //    {
                //        if (setState(State.ERROR))
                //        {
                //            log.error("Global thread has died. The instance will be in error state and should be closed.");
                //        }
                //    }
                //}
            }
        }
    }
}