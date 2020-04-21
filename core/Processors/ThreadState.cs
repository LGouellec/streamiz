using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// Stream thread states are the possible states that a stream thread can be in.
    /// A thread must only be in one state at a time
    /// The expected state transitions with the following defined states is:
    /// 
    ///                              +-------------+
    ///                    +&lt;-----| Created (0) |
    ///                    |         +-----+-------+
    ///                    |               |
    ///                    |               v
    ///                    |         +-----+-------+
    ///                    +&lt;---- | Starting (1)|-----&gt;+
    ///                    |         +-----+-------+         |
    ///                    |               |                 |
    ///                    |               |                 |
    ///                    |               v                 |
    ///                    |         +-----+-------+         |
    ///                    +&lt;---- | Partitions  |         |
    ///                    |         | Revoked (2) | &lt;----+
    ///                    |         +-----+-------+         |
    ///                    |             |  ^                |
    ///                    |             |  |                |
    ///                    |             v  |                |
    ///                    |         +-----+-------+         |
    ///                    +&lt;---- | Partitions  |         |
    ///                    |         | Assigned (3)| &lt;----+
    ///                    |         +-----+-------+         |
    ///                    |               |                 |
    ///                    |               |                 |
    ///                    |               v                 |
    ///                    |         +-----+-------+         |
    ///                    |         | Running (4) | ----&gt;+
    ///                    |         +-----+-------+
    ///                    |               |
    ///                    |               |
    ///                    |               v
    ///                    |         +-----+-------+
    ///                    +----&gt; | Pending     |
    ///                              | Shutdown (5)|
    ///                              +-----+-------+
    ///                                    |
    ///                                    v
    ///                              +-----+-------+
    ///                              | Dead (6)    |
    ///                              +-------------+
    ///          
    ///           Note the following:
    ///           <ul>
    ///               <li>Any state can go to PENDING_SHUTDOWN. That is because streams can be closed at any time.</li>
    ///               <li>
    ///                   State PENDING_SHUTDOWN may want to transit to some other states other than DEAD,
    ///                   in the corner case when the shutdown is triggered while the thread is still in the rebalance loop.
    ///                   In this case we will forbid the transition but will not treat as an error.
    ///               </li>
    ///               <li>
    ///                   State PARTITIONS_REVOKED may want transit to itself indefinitely, in the corner case when
    ///                   the coordinator repeatedly fails in-between revoking partitions and assigning new partitions.
    ///                   Also during streams instance start up PARTITIONS_REVOKED may want to transit to itself as well.
    ///                   In this case we will allow the transition but it will be a no-op as the set of revoked partitions
    ///                   should be empty.
    ///               </li>
    ///           </ul>
    /// 
    /// </summary>
    internal class ThreadState : ThreadStateTransitionValidator
    {
        public static ThreadState CREATED = new ThreadState(1, 5).Order(0).Name("CREATED");
        public static ThreadState STARTING = new ThreadState(2, 3, 5).Order(1).Name("STARTING");
        public static ThreadState PARTITIONS_REVOKED = new ThreadState(2, 3, 5).Order(2).Name("PARTITIONS_REVOKED");
        public static ThreadState PARTITIONS_ASSIGNED = new ThreadState(2, 3, 4, 5).Order(3).Name("PARTITIONS_ASSIGNED");
        public static ThreadState RUNNING = new ThreadState(2, 3, 5).Order(4).Name("RUNNING");
        public static ThreadState PENDING_SHUTDOWN = new ThreadState(6).Order(5).Name("PENDING_SHUTDOWN");
        public static ThreadState DEAD = new ThreadState().Order(6).Name("DEAD");

        private ISet<int> validTransitions = new HashSet<int>();
        private int ordinal = 0;
        private string name;

        private ThreadState(params int[] validTransitions)
        {
            this.validTransitions.AddRange(validTransitions);
        }

        private ThreadState Order(int ordinal)
        {
            this.ordinal = ordinal;
            return this;
        }

        private ThreadState Name(string name)
        {
            this.name = name;
            return this;
        }

        public bool IsRunning()
        {
            return this.Equals(RUNNING) || this.Equals(STARTING) || this.Equals(PARTITIONS_REVOKED) || this.Equals(PARTITIONS_ASSIGNED);
        }

        public bool IsValidTransition(ThreadStateTransitionValidator newState)
        {
            return validTransitions.Contains(((ThreadState)newState).ordinal);
        }

        public static bool operator ==(ThreadState a, ThreadState b) => a?.ordinal == b?.ordinal;
        public static bool operator !=(ThreadState a, ThreadState b) => a?.ordinal != b?.ordinal;

        public override bool Equals(object obj)
        {
            return obj is ThreadState && ((ThreadState)obj).ordinal.Equals(this.ordinal);
        }

        public override int GetHashCode()
        {
            return this.ordinal.GetHashCode();
        }

        public override string ToString()
        {
            return $"{this.name}";
        }
    }
}
