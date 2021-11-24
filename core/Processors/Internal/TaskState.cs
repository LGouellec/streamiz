using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal sealed class TaskState : TaskStateTransitionValidator, IEquatable<TaskState>
    {
        public static readonly TaskState CREATED = new TaskState(0, "CREATED", 1, 2, 3);
        public static readonly TaskState RESTORING = new TaskState(1, "RESTORING", 2, 3);
        public static readonly TaskState RUNNING = new TaskState(2, "RUNNING", 3);
        public static readonly TaskState SUSPENDED = new TaskState(3, "SUSPENDED", 1, 4);
        public static readonly TaskState CLOSED = new TaskState(4, "CLOSED", 0);

        /// <summary>
        /// Name of the state
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Order's state
        /// </summary>
        public int Ordinal { get; }

        /// <summary>
        /// Valid transition of the current state
        /// </summary>
        public ISet<int> Transitions { get; } = new HashSet<int>();

        private TaskState(int order, string name, params int[] validTransitions)
        {
            Ordinal = order;
            Name = name;
            Transitions.AddRange(validTransitions);
        }

        public bool IsValidTransition(TaskStateTransitionValidator newState)
        {
            return Transitions.Contains(((TaskState)newState).Ordinal);
        }

        public static bool operator ==(TaskState a, TaskState b) => a?.Ordinal == b?.Ordinal;
        public static bool operator !=(TaskState a, TaskState b) => a?.Ordinal != b?.Ordinal;

        public override bool Equals(object obj)
        {
            return obj is TaskState && ((TaskState)obj).Ordinal.Equals(Ordinal);
        }

        public override int GetHashCode()
        {
            return Ordinal.GetHashCode();
        }

        public override string ToString()
        {
            return $"{Name}";
        }

        public bool Equals(TaskState other) => Ordinal.Equals(other.Ordinal);
    }
}
