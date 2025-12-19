using System.Collections.Generic;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Tests.Helpers;

public class DictionaryExtensionsTests
{
    [Test]
    public void ToUpdateDictionaryWithMergeOperatorTest()
    {
        var dictionary = new List<KeyValuePair<string, long>>
        {
           new("a", 1L),
           new("b", 1L),
           new("c", 1L),
           new("d", 1L),
           new("a", 2L),
           new("b", 3L),
        };

        var results = dictionary.ToUpdateDictionary(
            kv => kv.Key,
            kv => kv.Value,
            (@old, @new) => @new);
        
        Assert.AreEqual(2L, results["a"]);
        Assert.AreEqual(3L, results["b"]);
        Assert.AreEqual(1L, results["c"]);
        Assert.AreEqual(1L, results["d"]);
    }
}