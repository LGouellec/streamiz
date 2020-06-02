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
        public static GlobalThreadState CREATED = new GlobalThreadState(1, 2).Order(0).Named("CREATED");
        public static GlobalThreadState RUNNING = new GlobalThreadState(2).Order(1).Named("RUNNING");
        public static GlobalThreadState PENDING_SHUTDOWN = new GlobalThreadState(3).Order(2).Named("PENDING_SHUTDOWN");
        public static GlobalThreadState DEAD = new GlobalThreadState().Order(3).Named("DEAD");

        /// <summary>
        /// Name of the state
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Order's state
        /// </summary>
        public int Ordinal { get; private set; }

        /// <summary>
        /// Valid transition of the current state
        /// </summary>
        public ISet<int> Transitions { get; } = new HashSet<int>();

        private GlobalThreadState(params int[] validTransitions)
        {
            Transitions.AddRange(validTransitions);
        }

        private GlobalThreadState Order(int ordinal)
        {
            Ordinal = ordinal;
            return this;
        }

        private GlobalThreadState Named(string name)
        {
            this.Name = name;
            return this;
        }

        public bool IsRunning()
        {
            return Equals(RUNNING);
        }

        public bool IsValidTransition(ThreadStateTransitionValidator newState)
        {
            return Transitions.Contains(((GlobalThreadState)newState).Ordinal);
        }

        public static bool operator ==(GlobalThreadState a, GlobalThreadState b) => a?.Ordinal == b?.Ordinal;
        public static bool operator !=(GlobalThreadState a, GlobalThreadState b) => a?.Ordinal != b?.Ordinal;

        public override bool Equals(object obj)
        {
            return obj is GlobalThreadState && ((GlobalThreadState)obj).Ordinal.Equals(Ordinal);
        }

        public override int GetHashCode()
        {
            return Ordinal.GetHashCode();
        }

        public override string ToString()
        {
            return $"{Name}";
        }
    }
}
