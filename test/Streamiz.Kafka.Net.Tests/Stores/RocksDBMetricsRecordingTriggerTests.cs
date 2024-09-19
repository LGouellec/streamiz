using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics.Internal;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class RocksDbMetricsRecordingTriggerTests
{
    private RocksDbMetricsRecordingTrigger recordingTrigger = new();
    
    private Mock<RocksDbMetricsRecorder> recorder1;
    private Mock<RocksDbMetricsRecorder> recorder2;
    
    private static string STORE_NAME1 = "store-name1";
    private static string STORE_NAME2 = "store-name2";

    [SetUp]
    public void Init()
    {
        recorder1 = new Mock<RocksDbMetricsRecorder>("rocksdb", STORE_NAME1);
        recorder1
            .Setup(d => d.AddValueProviders(It.IsAny<string>(), It.IsAny<IDbProperyProvider>()));
        recorder1
            .Setup(d => d.Name)
            .Returns($"0_0-{STORE_NAME1}");
                
        recorder2 = new Mock<RocksDbMetricsRecorder>("rocksdb", STORE_NAME2);
        recorder2
            .Setup(d => d.AddValueProviders(It.IsAny<string>(), It.IsAny<IDbProperyProvider>()));
        recorder2
            .Setup(d => d.Name)
            .Returns($"0_1-{STORE_NAME2}");
    }

    [TearDown]
    public void Dispose()
    {
        recordingTrigger = new();
    }

    [Test]
    public void TriggerAddedMetricsRecordersTest()
    {
        recordingTrigger.AddMetricsRecorder(recorder1.Object);
        recordingTrigger.AddMetricsRecorder(recorder2.Object);
        
        recordingTrigger.Run(100);
        
        recorder1.Verify(r => r.Record(100), Times.Once);
        recorder2.Verify(r => r.Record(100), Times.Once);
    }

    [Test]
    public void ThrowIfRecorderToAddHasBeenAlreadyAddedTest()
    {
        recordingTrigger.AddMetricsRecorder(recorder1.Object);
        Assert.Throws<IllegalStateException>(() => recordingTrigger.AddMetricsRecorder(recorder1.Object));
    }
}