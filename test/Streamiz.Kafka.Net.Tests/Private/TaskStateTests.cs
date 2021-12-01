using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class TaskStateTests
    {
        [Test]
        public void IsTrueEqual()
        {
            Assert.IsTrue(TaskState.CREATED == TaskState.CREATED);
            Assert.IsTrue(TaskState.CLOSED == TaskState.CLOSED);
            Assert.IsTrue(TaskState.RESTORING == TaskState.RESTORING);
            Assert.IsTrue(TaskState.RUNNING == TaskState.RUNNING);
            Assert.IsTrue(TaskState.SUSPENDED == TaskState.SUSPENDED);
        }

        [Test]
        public void IsFalseEqual()
        {
            Assert.IsTrue(TaskState.CREATED != TaskState.RESTORING);
            Assert.IsTrue(TaskState.RESTORING != TaskState.RUNNING);
            Assert.IsTrue(TaskState.CLOSED != TaskState.RESTORING);
            Assert.IsTrue(TaskState.SUSPENDED != TaskState.CLOSED);
            Assert.IsTrue(TaskState.RUNNING != TaskState.CLOSED);
        }

        [Test]
        public void Equal()
        {
            TaskState.CREATED.GetHashCode();
            TaskState.CREATED.ToString();
            Assert.IsTrue(TaskState.CREATED.Equals(TaskState.CREATED));
            TaskState.CLOSED.GetHashCode();
            TaskState.CLOSED.ToString();
            Assert.IsTrue(TaskState.CLOSED.Equals(TaskState.CLOSED));
            TaskState.RESTORING.GetHashCode();
            TaskState.RESTORING.ToString();
            Assert.IsTrue(TaskState.RESTORING.Equals(TaskState.RESTORING));
            TaskState.RUNNING.GetHashCode();
            TaskState.RUNNING.ToString();
            Assert.IsTrue(TaskState.RUNNING.Equals(TaskState.RUNNING));
            TaskState.SUSPENDED.GetHashCode();
            TaskState.SUSPENDED.ToString();
            Assert.IsTrue(TaskState.SUSPENDED.Equals(TaskState.SUSPENDED));
        }

        [Test]
        public void ValidTransition()
        {
            Assert.IsTrue(TaskState.CREATED.IsValidTransition(TaskState.RUNNING));
            Assert.IsTrue(TaskState.CREATED.IsValidTransition(TaskState.RESTORING));
            Assert.IsTrue(TaskState.CREATED.IsValidTransition(TaskState.SUSPENDED));

            Assert.IsTrue(TaskState.RESTORING.IsValidTransition(TaskState.RUNNING));
            Assert.IsTrue(TaskState.RESTORING.IsValidTransition(TaskState.SUSPENDED));

            Assert.IsTrue(TaskState.RUNNING.IsValidTransition(TaskState.SUSPENDED));

            Assert.IsTrue(TaskState.SUSPENDED.IsValidTransition(TaskState.CLOSED));
            Assert.IsTrue(TaskState.SUSPENDED.IsValidTransition(TaskState.RESTORING));

            Assert.IsTrue(TaskState.CLOSED.IsValidTransition(TaskState.CREATED));
        }

        [Test]
        public void InvalidTransition()
        {
            Assert.IsFalse(TaskState.CREATED.IsValidTransition(TaskState.CLOSED));

            Assert.IsFalse(TaskState.RESTORING.IsValidTransition(TaskState.CLOSED));
            Assert.IsFalse(TaskState.RESTORING.IsValidTransition(TaskState.CREATED));

            Assert.IsFalse(TaskState.RUNNING.IsValidTransition(TaskState.CLOSED));
            Assert.IsFalse(TaskState.RUNNING.IsValidTransition(TaskState.CREATED));
            Assert.IsFalse(TaskState.RUNNING.IsValidTransition(TaskState.RESTORING));

            Assert.IsFalse(TaskState.SUSPENDED.IsValidTransition(TaskState.CREATED));
            Assert.IsFalse(TaskState.SUSPENDED.IsValidTransition(TaskState.RUNNING));

            Assert.IsFalse(TaskState.CLOSED.IsValidTransition(TaskState.RUNNING));
            Assert.IsFalse(TaskState.CLOSED.IsValidTransition(TaskState.RESTORING));
            Assert.IsFalse(TaskState.CLOSED.IsValidTransition(TaskState.SUSPENDED));
        }
    }
}