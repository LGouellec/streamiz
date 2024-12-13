using System;
using System.IO;
using NUnit.Framework;
using RocksDbSharp;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Tests.Private;

public class StateDirectoryTests
{
    [Test]
    [NonParallelizable]
    public void GettingAnExistingProcessorId()
    {
        if(File.Exists("./unit-test-1/streamiz-process-metadata"))
            File.Delete("./unit-test-1/streamiz-process-metadata");
        
        var config = new StreamConfig();
        config.ApplicationId = "test-state-dir";
        config.StateDir = "./unit-test-1";
        
        var instance = new StateDirectory(config, true);
        var newGuid = Guid.NewGuid();
        Directory.CreateDirectory("./unit-test-1");
        File.WriteAllText("./unit-test-1/streamiz-process-metadata", "{" + @"""processId"": """ + $"{newGuid}"+ @"""" + "}");
        var guid = instance.InitializeProcessId();
        
        Directory.Delete("./unit-test-1", true);
        Assert.AreEqual(newGuid, guid);
    }
    
    [Test]
    [NonParallelizable]
    public void FailedDeseriliazeProcessorId()
    {
        if(File.Exists("./unit-test-2/streamiz-process-metadata"))
            File.Delete("./unit-test-2/streamiz-process-metadata");
        
        var config = new StreamConfig();
        config.ApplicationId = "test-state-dir";
        config.StateDir = "./unit-test-2";
        
        var instance = new StateDirectory(config, true);
        var newGuid = Guid.NewGuid();
        Directory.CreateDirectory("./unit-test-2");
        File.WriteAllText("./unit-test-2/streamiz-process-metadata", $"{newGuid}");
        var guid = instance.InitializeProcessId();
        
        Directory.Delete("./unit-test-2", true);
        Assert.AreNotEqual(newGuid, guid);
    }
    
    [Test]
    public void NoPersistenStoreProcessorId()
    {
        var config = new StreamConfig();
        config.ApplicationId = "test-state-dir";
        config.StateDir = ".";
        
        var instance = new StateDirectory(config, false);
        var guid1 = instance.InitializeProcessId();
        var guid2 = instance.InitializeProcessId();
        
        Assert.AreNotEqual(guid1, guid2);
    }
}