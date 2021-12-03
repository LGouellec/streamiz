using System;
using NUnit.Framework;
using Streamiz.Kafka.Net.Processors.Internal;
using System.IO;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class GlobalProcessorContextTests
    {
        [Test]
        public void TestGlobalProcessorContextStateDir()
        {
            var streamConfig = new StreamConfig();
            streamConfig.ApplicationId = "test-global-processor-context";
            streamConfig.StateDir = Path.Combine(".", Guid.NewGuid().ToString());
            
            var context = new GlobalProcessorContext(streamConfig, new GlobalStateManager(null, null, null));
            
            Assert.AreEqual(Path.Combine(streamConfig.StateDir, streamConfig.ApplicationId, "global"), context.StateDir);
        }
        
        [Test]
        public void TestGlobalProcessorContextTaskId()
        {
            var streamConfig = new StreamConfig();
            streamConfig.ApplicationId = "test-global-processor-context";
            streamConfig.StateDir = Path.Combine(".", Guid.NewGuid().ToString());
            
            var context = new GlobalProcessorContext(streamConfig, new GlobalStateManager(null, null, null));
            
            Assert.AreEqual(new TaskId{Id = -1, Partition = -1}, context.Id);
            Assert.IsNull(context.Task);
        }
    }
}