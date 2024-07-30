using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Cache.Internal;

namespace Streamiz.Kafka.Net.Tests.Private;

// TODO : 
public class ConcurrentSortedDictionaryTests
{
    private ConcurrentSortedDictionary<Bytes, string> concurrentSet;

    [SetUp]
    public void Init()
    {
        concurrentSet = new ConcurrentSortedDictionary<Bytes, string>();
    }

    [TearDown]
    public void Dispose()
    {
        concurrentSet.Clear();
    }
}