using Confluent.Kafka;
using System;
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
        /// Wait until <paramref name="size"/> messages in topic and 10 timeout seconds.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="topic">output topic</param>
        /// <param name="size">number of message waiting</param>
        /// <returns>Return a list of records</returns>
        public static List<ConsumeResult<K, V>> WaitUntilMinKeyValueRecordsReceived<K, V>(TestOutputTopic<K, V> topic, int size)
        {
            DateTime dt = DateTime.Now;
            TimeSpan ts = TimeSpan.FromSeconds(10);

            List<ConsumeResult<K, V>> results = new List<ConsumeResult<K, V>>();
            do
            {
                results.AddRange(topic.ReadKeyValueList().ToList());
                if (dt + ts < DateTime.Now)
                    break;
            } while (results.Count < size);

            return results;
        }

        /// <summary>
        /// Read output to map.
        /// This method can be used if the result is considered a table, when you are only interested in the last table update (ie, the final table state).
        /// 
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <typeparam name="V">value type</typeparam>
        /// <param name="outputTopic">output topic</param>
        /// <returns>Map of output topic by key.</returns>
        public static Dictionary<K, V> ReadKeyValuesToMap<K, V>(this TestOutputTopic<K, V> outputTopic)
        {
            Dictionary<K, V> map = new Dictionary<K, V>();

            var result = outputTopic.ReadKeyValueList();
            foreach (var r in result)
            {
                if (map.ContainsKey(r.Message.Key))
                {
                    map[r.Message.Key] = r.Message.Value;
                }
                else
                {
                    map.Add(r.Message.Key, r.Message.Value);
                }
            }

            return map;
        }
    }
}
