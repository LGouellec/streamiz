using Confluent.Kafka;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Mock
{
    public static class IntegrationTestUtils
    {
        public static List<ConsumeResult<K, V>> WaitUntilMinKeyValueRecordsReceived<K, V>(TestOutputTopic<K, V> topic, int size)
        {
            List<ConsumeResult<K, V>> results = new List<ConsumeResult<K, V>>();
            do
            {
                results.AddRange(topic.ReadKeyValueList().ToList());
            } while (results.Count < size);

            return results;
        }
    }
}
