﻿using System;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The <see cref="IValueMapperWithKey{K, V, VR}"/> interface for mapping a value to a new value of arbitrary type.
    /// This is a stateless record-by-record operation, i.e, <see cref="IValueMapperWithKey{K, V, VR}.Apply(K, V)"/> is invoked individually for each
    /// record of a stream.
    /// If <see cref="IValueMapperWithKey{K, V, VR}"/> is applied to a keyvalue pair record the
    /// record's key is preserved.
    /// Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
    /// If a record's key and value should be modified <see cref="IKeyValueMapper{K, V, VR}"/> can be used.
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">value type</typeparam>
    /// <typeparam name="VR">mapped value type</typeparam>
    public interface IValueMapperWithKey<in K, in V, out VR>
    {
        /// <summary>
        /// Map the given [key and ]value to a new value.
        /// </summary>
        /// <param name="keyReadonly">the readonly key</param>
        /// <param name="value">the value to be mapped</param>
        /// <param name="context">the current context</param>
        /// <returns>the new value</returns>
        VR Apply(K keyReadonly, V value, IRecordContext context);
    }

    internal class WrappedValueMapperWithKey<K, V, VR> : IValueMapperWithKey<K, V, VR>
    {
        private readonly Func<K, V, IRecordContext, VR> mapper;

        public WrappedValueMapperWithKey(Func<K, V, IRecordContext, VR> mapper)
        {
            this.mapper = mapper ?? throw new ArgumentNullException($"Mapper function can't be null");
        }

        public VR Apply(K readOnlyKey, V value, IRecordContext context) => this.mapper(readOnlyKey, value, context);
    }
}
