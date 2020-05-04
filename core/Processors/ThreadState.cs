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
        public static readonly ThreadState CREATED = new ThreadState(0, "CREATED", 1, 5);
        public static readonly ThreadState STARTING = new ThreadState(1, "STARTING", 2, 3, 5);
        public static readonly ThreadState PARTITIONS_REVOKED = new ThreadState(2, "PARTITIONS_REVOKED", 2, 3, 5);
        public static readonly ThreadState PARTITIONS_ASSIGNED = new ThreadState(3, "PARTITIONS_ASSIGNED", 2, 3, 4, 5);
        public static readonly ThreadState RUNNING = new ThreadState(4, "RUNNING", 2, 3, 5);
        public static readonly ThreadState PENDING_SHUTDOWN = new ThreadState(5, "PENDING_SHUTDOWN", 6);
        public static readonly ThreadState DEAD = new ThreadState(6, "DEAD");

        private readonly ISet<int> validTransitions = new HashSet<int>();
        private readonly int ordinal = 0;
        private readonly string name;

        private ThreadState(int order, string name, params int[] validTransitions)
        {
            ordinal = order;
            this.name = name;
            this.validTransitions.AddRange(validTransitions);
        }

        public bool IsRunning()
        {
            return Equals(RUNNING) || Equals(STARTING) || Equals(PARTITIONS_REVOKED) || Equals(PARTITIONS_ASSIGNED);
        }

        public bool IsValidTransition(ThreadStateTransitionValidator newState)
        {
            return validTransitions.Contains(((ThreadState)newState).ordinal);
        }

        public static bool operator ==(ThreadState a, ThreadState b) => a?.ordinal == b?.ordinal;
        public static bool operator !=(ThreadState a, ThreadState b) => a?.ordinal != b?.ordinal;

        public override bool Equals(object obj)
        {
            return obj is ThreadState && ((ThreadState)obj).ordinal.Equals(ordinal);
        }

        public override int GetHashCode()
        {
            return ordinal.GetHashCode();
        }

        public override string ToString()
        {
            return $"{name}";
        }
    }
}
