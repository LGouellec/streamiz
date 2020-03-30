using kafka_stream_core.Processors;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Stream
{
    public interface KStream<K, V>
    {
        KStream<K, V>[] branch(params Func<K, V, bool>[] predicates);
        KStream<K, V>[] branch(string named, params Func<K, V, bool>[] predicates);
        KStream<K, V> filter(Func<K, V, bool> predicate);
        KStream<K, V> filterNot(Func<K, V, bool> predicate);
        void to(string topicName, Produced<K, V> produced);
        void to(string topicName);
        void to(TopicNameExtractor<K, V> topicExtractor);
        void to(TopicNameExtractor<K, V> topicExtractor, Produced<K, V> produced);
        KStream<KR, VR> flatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper);
        KStream<KR, VR> flatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named);
        KStream<KR, VR> flatMap<KR, VR>(Func<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper);
        KStream<KR, VR> flatMap<KR, VR>(Func<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named);
        KStream<K, VR> flatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper);
        KStream<K, VR> flatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper, string named);
        KStream<K, VR> flatMapValues<VR>(Func<V, IEnumerable<VR>> mapper);
        KStream<K, VR> flatMapValues<VR>(Func<V, IEnumerable<VR>> mapper, string named);
        KStream<K, VR> flatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper);
        KStream<K, VR> flatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper, string named);
        KStream<K, VR> flatMapValues<VR>(Func<K, V, IEnumerable<VR>> mapper);
        KStream<K, VR> flatMapValues<VR>(Func<K, V, IEnumerable<VR>> mapper, string named);
        void @foreach(Action<K, V> action);
        void @foreach(Action<K, V> action, string named);
        KStream<KR, VR> map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper);
        KStream<KR, VR> map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper, string named);
        KStream<KR, VR> map<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> mapper);
        KStream<KR, VR> map<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> mapper, string named);
        KStream<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper);
        KStream<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper, string named);
        KStream<K, VR> mapValues<VR>(Func<V, VR> mapper);
        KStream<K, VR> mapValues<VR>(Func<V, VR> mapper, string named);
        KStream<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapper);
        KStream<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, string named);
        KStream<K, VR> mapValues<VR>(Func<K, V, VR> mapper);
        KStream<K, VR> mapValues<VR>(Func<K, V, VR> mapper, string named);
        KStream<K, V> peek(Action<K, V> action);
        KStream<K, V> peek(Action<K, V> action, string named);
        KStream<KR, V> selectKey<KR>(IKeyValueMapper<K, V, KR> mapper);
        KStream<KR, V> selectKey<KR>(IKeyValueMapper<K, V, KR> mapper, string named);
        KStream<KR, V> selectKey<KR>(Func<K, V, KR> mapper);
        KStream<KR, V> selectKey<KR>(Func<K, V, KR> mapper, string named);
    }
}
