using System;

namespace Streamiz.Kafka.Net.Mock
{
    /// <summary>
    /// A key/value pair, including timestamp, to be sent to or received from <see cref="TopologyTestDriver"/>.
    /// If [a] record does not contain a timestamp, will auto advance it's time when the record is piped.
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">value type</typeparam>
    public class TestRecord<K, V>
    {
        /// <summary>
        /// Key or null if no key is specified
        /// </summary>
        public K Key { get; set; }

        /// <summary>
        /// Value
        /// </summary>
        public V Value { get; set; }

        /// <summary>
        /// The timestamp or null if no timestamp was setted
        /// </summary>
        public DateTime? Timestamp { get; set; }


        /// <summary>
        /// Empty constructor
        /// </summary>
        public TestRecord()
        {

        }

        /// <summary>
        /// Constructor with key/value and timestamp
        /// </summary>
        /// <param name="key">Key of record</param>
        /// <param name="value">Value of record</param>
        /// <param name="timestamp">Timestamp of record</param>
        public TestRecord(K key, V value, DateTime? timestamp)
        {
            Key = key;
            Value = value;
            Timestamp = timestamp;
        }

        /// <summary>
        /// Constructor with key/value
        /// </summary>
        /// <param name="key">Key of record</param>
        /// <param name="value">Value of record</param>
        public TestRecord(K key, V value)
        {
            Key = key;
            Value = value;
        }
    }
}
