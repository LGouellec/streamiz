using NUnit.Framework;
using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class GlobalThreadStateTests
    {
        [Test]
        public void CreatedCanTransitionToRunningAndPendingShutdown()
        {
            Assert.IsTrue(GlobalThreadState.CREATED.IsValidTransition(GlobalThreadState.RUNNING));
            Assert.IsTrue(GlobalThreadState.CREATED.IsValidTransition(GlobalThreadState.PENDING_SHUTDOWN));
        }

        [Test]
        public void CreatedCanNotTransitionToDead()
        {
            Assert.IsFalse(GlobalThreadState.CREATED.IsValidTransition(GlobalThreadState.DEAD));
        }

        [Test]
        public void RunningCanTransitionToPendingShutdown()
        {
            Assert.IsTrue(GlobalThreadState.RUNNING.IsValidTransition(GlobalThreadState.PENDING_SHUTDOWN));
        }

        [Test]
        public void RunningCanNotTransitionToDeadAndCreated()
        {
            Assert.IsFalse(GlobalThreadState.RUNNING.IsValidTransition(GlobalThreadState.DEAD));
            Assert.IsFalse(GlobalThreadState.RUNNING.IsValidTransition(GlobalThreadState.CREATED));
        }

        [Test]
        public void PendingShutdownCanTransitionToDead()
        {
            Assert.IsTrue(GlobalThreadState.PENDING_SHUTDOWN.IsValidTransition(GlobalThreadState.DEAD));
        }

        [Test]
        public void PendingShutdownCanNotTransitionToCreatedAndRunning()
        {
            Assert.IsFalse(GlobalThreadState.PENDING_SHUTDOWN.IsValidTransition(GlobalThreadState.CREATED));
            Assert.IsFalse(GlobalThreadState.PENDING_SHUTDOWN.IsValidTransition(GlobalThreadState.RUNNING));
        }

        [Test]
        public void DeadCanNotTransitionToCreatedPendingShutdownAndRunning()
        {
            Assert.IsFalse(GlobalThreadState.DEAD.IsValidTransition(GlobalThreadState.CREATED));
            Assert.IsFalse(GlobalThreadState.DEAD.IsValidTransition(GlobalThreadState.RUNNING));
            Assert.IsFalse(GlobalThreadState.DEAD.IsValidTransition(GlobalThreadState.PENDING_SHUTDOWN));
        }
    }
}
