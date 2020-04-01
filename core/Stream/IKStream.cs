using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Stream
{
    public interface IKStream<K, V>
    {
        IKStream<K, V>[] Branch(params Func<K, V, bool>[] predicates);
        IKStream<K, V>[] Branch(string named, params Func<K, V, bool>[] predicates);
        IKStream<K, V> Filter(Func<K, V, bool> predicate);
        IKStream<K, V> Filter(Func<K, V, bool> predicate, string named);
        IKStream<K, V> FilterNot(Func<K, V, bool> predicate);
        IKStream<K, V> FilterNot(Func<K, V, bool> predicate, string named);
        void To(string topicName, Produced<K, V> produced);
        void To(string topicName);
        void To(Func<K, V, IRecordContext, string> topicExtractor);
        void To(Func<K, V, IRecordContext, string> topicExtractor, Produced<K, V> produced);
        void To(ITopicNameExtractor<K, V> topicExtractor);
        void To(ITopicNameExtractor<K, V> topicExtractor, Produced<K, V> produced);
        IKStream<KR, VR> FlatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper);
        IKStream<KR, VR> FlatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named);
        IKStream<KR, VR> FlatMap<KR, VR>(Func<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper);
        IKStream<KR, VR> FlatMap<KR, VR>(Func<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named);
        IKStream<K, VR> FlatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper);
        IKStream<K, VR> FlatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper, string named);
        IKStream<K, VR> FlatMapValues<VR>(Func<V, IEnumerable<VR>> mapper);
        IKStream<K, VR> FlatMapValues<VR>(Func<V, IEnumerable<VR>> mapper, string named);
        IKStream<K, VR> FlatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper);
        IKStream<K, VR> FlatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper, string named);
        IKStream<K, VR> FlatMapValues<VR>(Func<K, V, IEnumerable<VR>> mapper);
        IKStream<K, VR> FlatMapValues<VR>(Func<K, V, IEnumerable<VR>> mapper, string named);
        void Foreach(Action<K, V> action);
        void Foreach(Action<K, V> action, string named);
        void Print(Printed<K, V> printed);
        IKStream<KR, VR> Map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper);
        IKStream<KR, VR> Map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper, string named);
        IKStream<KR, VR> Map<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> mapper);
        IKStream<KR, VR> Map<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> mapper, string named);
        IKStream<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper);
        IKStream<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, string named);
        IKStream<K, VR> MapValues<VR>(Func<V, VR> mapper);
        IKStream<K, VR> MapValues<VR>(Func<V, VR> mapper, string named);
        IKStream<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapper);
        IKStream<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, string named);
        IKStream<K, VR> MapValues<VR>(Func<K, V, VR> mapper);
        IKStream<K, VR> MapValues<VR>(Func<K, V, VR> mapper, string named);
        IKStream<K, V> Peek(Action<K, V> action);
        IKStream<K, V> Peek(Action<K, V> action, string named);
        IKStream<KR, V> SelectKey<KR>(IKeyValueMapper<K, V, KR> mapper);
        IKStream<KR, V> SelectKey<KR>(IKeyValueMapper<K, V, KR> mapper, string named);
        IKStream<KR, V> SelectKey<KR>(Func<K, V, KR> mapper);
        IKStream<KR, V> SelectKey<KR>(Func<K, V, KR> mapper, string named);
        IKGroupedStream<KR, V> GroupBy<KR>(IKeyValueMapper<K, V, KR> keySelector);
        IKGroupedStream<KR, V> GroupBy<KR>(Func<K, V, KR> keySelector);
        IKGroupedStream<KR, V> GroupBy<KR>(IKeyValueMapper<K, V, KR> keySelector, Grouped<KR, V> grouped);
        IKGroupedStream<KR, V> GroupBy<KR>(Func<K, V, KR> keySelector, Grouped<KR, V> grouped);
        IKGroupedStream<K, V> GroupByKey();
        IKGroupedStream<K, V> GroupByKey(Grouped<K, V> grouped);
    }
}
