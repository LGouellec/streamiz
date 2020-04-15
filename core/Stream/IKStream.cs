using Kafka.Streams.Net.Processors;
using Kafka.Streams.Net.SerDes;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Net.Stream
{
    public interface IKStream<K, V>
    {
        IKStream<K, V>[] Branch(params Func<K, V, bool>[] predicates);
        IKStream<K, V>[] Branch(string named, params Func<K, V, bool>[] predicates);
        IKStream<K, V> Filter(Func<K, V, bool> predicate, string named = null);
        IKStream<K, V> FilterNot(Func<K, V, bool> predicate, string named = null);
        void To(string topicName, string named = null);
        void To(Func<K, V, IRecordContext, string> topicExtractor, string named = null);
        void To(ITopicNameExtractor<K, V> topicExtractor, string named = null);
        void To<KS, VS>(string topicName, string named = null) where KS : ISerDes<K>, new() where VS : ISerDes<V>, new();
        void To<KS, VS>(Func<K, V, IRecordContext, string> topicExtractor, string named = null) where KS : ISerDes<K>, new() where VS : ISerDes<V>, new();
        void To<KS, VS>(ITopicNameExtractor<K, V> topicExtractor, string named = null) where KS : ISerDes<K>, new() where VS : ISerDes<V>, new();
        IKStream<KR, VR> FlatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named = null);
        IKStream<KR, VR> FlatMap<KR, VR>(Func<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named = null);
        IKStream<K, VR> FlatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper, string named = null);
        IKStream<K, VR> FlatMapValues<VR>(Func<V, IEnumerable<VR>> mapper, string named = null);
        IKStream<K, VR> FlatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper, string named = null);
        IKStream<K, VR> FlatMapValues<VR>(Func<K, V, IEnumerable<VR>> mapper, string named = null);
        void Foreach(Action<K, V> action, string named = null);
        void Print(Printed<K, V> printed);
        IKStream<KR, VR> Map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper, string named = null);
        IKStream<KR, VR> Map<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> mapper, string named = null);
        IKStream<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, string named = null);
        IKStream<K, VR> MapValues<VR>(Func<V, VR> mapper, string named = null);
        IKStream<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, string named = null);
        IKStream<K, VR> MapValues<VR>(Func<K, V, VR> mapper, string named = null);
        IKStream<K, V> Peek(Action<K, V> action, string named = null);
        IKStream<KR, V> SelectKey<KR>(IKeyValueMapper<K, V, KR> mapper, string named = null);
        IKStream<KR, V> SelectKey<KR>(Func<K, V, KR> mapper, string named = null);
        IKGroupedStream<KR, V> GroupBy<KR>(IKeyValueMapper<K, V, KR> keySelector, string named = null);
        IKGroupedStream<KR, V> GroupBy<KR>(Func<K, V, KR> keySelector, string named = null);
        IKGroupedStream<KR, V> GroupBy<KR, KRS>(IKeyValueMapper<K, V, KR> keySelector, string named = null) where KRS : ISerDes<KR>, new();
        IKGroupedStream<KR, V> GroupBy<KR, KRS>(Func<K, V, KR> keySelector, string named = null) where KRS : ISerDes<KR>, new();
        IKGroupedStream<K, V> GroupByKey(string named = null);
        IKGroupedStream<K, V> GroupByKey<KS, VS>(string named = null) where KS : ISerDes<K>, new() where VS : ISerDes<V>, new();
    }
}
