using Confluent.Kafka;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Mock
{
    /// <summary>
    /// Integrations test utils class
    /// </summary>
    public static class IntegrationTestUtils
    {
        /// <summary>
        /// Wait until <paramref name="size"/> messages in topic
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="topic">output topic</param>
        /// <param name="size">number of message waiting</param>
        /// <returns>Return a list of records</returns>
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
