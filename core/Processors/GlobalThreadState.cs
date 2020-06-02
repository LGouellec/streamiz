using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// The states that the global stream thread can be in
    /// 
    ///                              +-------------+
    ///                    +&lt;-----| Created (0) |
    ///                    |         +-----+-------+
    ///                    |               |
    ///                    |               |
    ///                    |               v
    ///                    |         +-----+-------+
    ///                    |&lt;-----| Running (1) |
    ///                    |         +-----+-------+
    ///                    |               |
    ///                    |               |
    ///                    |               v
    ///                    |         +-----+-------+
    ///                    +----&gt; | Pending     |
    ///                              | Shutdown (2)|
    ///                              +-----+-------+
    ///                                    |
    ///                                    v
    ///                              +-----+-------+
    ///                              | Dead (3)    |
    ///                              +-------------+
    ///          
    ///           Note the following:
    ///           <ul>
    ///               <li>Any state can go to PENDING_SHUTDOWN. That is because streams can be closed at any time.</li>
    ///               <li>State PENDING_SHUTDOWN may want to transit itself. In this case we will forbid the transition but will not treat as an error.</li>
    ///           </ul>
    /// 
    /// </summary>
    internal class GlobalThreadState : ThreadStateTransitionValidator
    {
        public static GlobalThreadState CREATED = new GlobalThreadState(1, 2).Order(0).Name("CREATED");
        public static GlobalThreadState RUNNING = new GlobalThreadState(2).Order(1).Name("RUNNING");
        public static GlobalThreadState PENDING_SHUTDOWN = new GlobalThreadState(3).Order(2).Name("PENDING_SHUTDOWN");
        public static GlobalThreadState DEAD = new GlobalThreadState().Order(3).Name("DEAD");

        private ISet<int> validTransitions = new HashSet<int>();
        private int ordinal = 0;
        private string name;

        private GlobalThreadState(params int[] validTransitions)
        {
            this.validTransitions.AddRange(validTransitions);
        }

        private GlobalThreadState Order(int ordinal)
        {
            this.ordinal = ordinal;
            return this;
        }

        private GlobalThreadState Name(string name)
        {
            this.name = name;
            return this;
        }

        public bool IsRunning()
        {
            return this.Equals(RUNNING);
        }

        public bool IsValidTransition(ThreadStateTransitionValidator newState)
        {
            return validTransitions.Contains(((GlobalThreadState)newState).ordinal);
        }

        public static bool operator ==(GlobalThreadState a, GlobalThreadState b) => a?.ordinal == b?.ordinal;
        public static bool operator !=(GlobalThreadState a, GlobalThreadState b) => a?.ordinal != b?.ordinal;

        public override bool Equals(object obj)
        {
            return obj is GlobalThreadState && ((GlobalThreadState)obj).ordinal.Equals(this.ordinal);
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
