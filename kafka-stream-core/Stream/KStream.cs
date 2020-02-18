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
        KStream<KR, VR> flatMap<KR, VR>(KeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper);
        KStream<KR, VR> flatMap<KR, VR>(KeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named);
        void @foreach(Action<K, V> action);
        void @foreach(Action<K, V> action, string named);
        KStream<KR, VR> map<KR, VR>(KeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper);
        KStream<KR, VR> map<KR, VR>(KeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper, string named);
        KStream<K, V> peek(Action<K, V> action);
        KStream<K, V> peek(Action<K, V> action, string named);
        KStream<KR, V> selectKey<KR>(KeyValueMapper<K, V, KR> mapper);
        KStream<KR, V> selectKey<KR>(KeyValueMapper<K, V, KR> mapper, string named);
    }
}
