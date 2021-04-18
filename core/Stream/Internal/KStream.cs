using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;
using Streamiz.Kafka.Net.Table.Internal.Graph;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal static class KStream
    {
        #region Constants

        internal static readonly string JOINTHIS_NAME = "KSTREAM-JOINTHIS-";
        internal static readonly string JOINOTHER_NAME = "KSTREAM-JOINOTHER-";
        internal static readonly string JOIN_NAME = "KSTREAM-JOIN-";
        internal static readonly string LEFTJOIN_NAME = "KSTREAM-LEFTJOIN-";
        internal static readonly string MERGE_NAME = "KSTREAM-MERGE-";
        internal static readonly string OUTERTHIS_NAME = "KSTREAM-OUTERTHIS-";
        internal static readonly string OUTEROTHER_NAME = "KSTREAM-OUTEROTHER-";
        internal static readonly string WINDOWED_NAME = "KSTREAM-WINDOWED-";
        internal static readonly string SOURCE_NAME = "KSTREAM-SOURCE-";
        internal static readonly string SINK_NAME = "KSTREAM-SINK-";
        internal static readonly string REPARTITION_TOPIC_SUFFIX = "-repartition";
        internal static readonly string BRANCH_NAME = "KSTREAM-BRANCH-";
        internal static readonly string BRANCHCHILD_NAME = "KSTREAM-BRANCHCHILD-";
        internal static readonly string FILTER_NAME = "KSTREAM-FILTER-";
        internal static readonly string PEEK_NAME = "KSTREAM-PEEK-";
        internal static readonly string FLATMAP_NAME = "KSTREAM-FLATMAP-";
        internal static readonly string FLATMAPVALUES_NAME = "KSTREAM-FLATMAPVALUES-";
        internal static readonly string MAP_NAME = "KSTREAM-MAP-";
        internal static readonly string MAPVALUES_NAME = "KSTREAM-MAPVALUES-";
        internal static readonly string PROCESSOR_NAME = "KSTREAM-PROCESSOR-";
        internal static readonly string PRINTING_NAME = "KSTREAM-PRINTER-";
        internal static readonly string KEY_SELECT_NAME = "KSTREAM-KEY-SELECT-";
        internal static readonly string TRANSFORM_NAME = "KSTREAM-TRANSFORM-";
        internal static readonly string TRANSFORMVALUES_NAME = "KSTREAM-TRANSFORMVALUES-";
        internal static readonly string FOREACH_NAME = "KSTREAM-FOREACH-";
        internal static readonly string TO_KTABLE_NAME = "KSTREAM-TOTABLE-";

        #endregion
    }

    internal class KStream<K, V> : AbstractStream<K, V>, IKStream<K, V>
    {
        internal KStream(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> setSourceNodes, StreamGraphNode node, InternalStreamBuilder builder)
            : base(name, keySerdes, valueSerdes, setSourceNodes, node, builder)
        {
        }

        #region Branch

        public IKStream<K, V>[] Branch(params Func<K, V, bool>[] predicates) => DoBranch(string.Empty, predicates);

        public IKStream<K, V>[] Branch(string named, params Func<K, V, bool>[] predicates) => DoBranch(named, predicates);

        #endregion

        #region Filter

        public IKStream<K, V> Filter(Func<K, V, bool> predicate, string named = null)
            => DoFilter(predicate, named, false);

        public IKStream<K, V> FilterNot(Func<K, V, bool> predicate, string named = null)
            => DoFilter(predicate, named, true);

        #endregion

        #region Transform

        #endregion

        #region To

        public void To(string topicName, string named = null)
        {
            if (topicName == null)
            {
                throw new ArgumentNullException(nameof(topicName));
            }

            if (string.IsNullOrEmpty(topicName))
            {
                throw new ArgumentException("topicName must be empty");
            }

            To(new StaticTopicNameExtractor<K, V>(topicName), named);
        }

        public void To(ITopicNameExtractor<K, V> topicExtractor, string named = null) => DoTo(topicExtractor, Produced<K, V>.Create(KeySerdes, ValueSerdes).WithName(named));

        public void To(Func<K, V, IRecordContext, string> topicExtractor, string named = null) => To(new WrapperTopicNameExtractor<K, V>(topicExtractor), named);

        public void To<KS, VS>(Func<K, V, IRecordContext, string> topicExtractor, string named = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => To<KS, VS>(new WrapperTopicNameExtractor<K, V>(topicExtractor), named);

        public void To<KS, VS>(string topicName, string named = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => To<KS, VS>(new StaticTopicNameExtractor<K, V>(topicName), named);

        public void To<KS, VS>(ITopicNameExtractor<K, V> topicExtractor, string named = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => DoTo(topicExtractor, Produced<K, V>.Create<KS, VS>().WithName(named));

        #endregion

        #region FlatMap

        public IKStream<KR, VR> FlatMap<KR, VR>(Func<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named = null)
            => FlatMap(new WrappedKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>>(mapper), named);

        public IKStream<KR, VR> FlatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named = null)
        {
            if (mapper == null)
            {
                throw new ArgumentNullException($"FlatMap() doesn't allow null mapper function");
            }

            var name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.FLATMAP_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMap<K, V, KR, VR>(mapper), name);
            ProcessorGraphNode<K, V> flatMapNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            flatMapNode.KeyChangingOperation = true;

            builder.AddGraphNode(Node, flatMapNode);

            // key and value serde cannot be preserved
            return new KStream<KR, VR>(name, null, null, SetSourceNodes, flatMapNode, builder);
        }

        #endregion

        #region FlatMapValues

        public IKStream<K, VR> FlatMapValues<VR>(Func<V, IEnumerable<VR>> mapper, string named = null)
            => FlatMapValues(new WrappedValueMapper<V, IEnumerable<VR>>(mapper), named);

        public IKStream<K, VR> FlatMapValues<VR>(Func<K, V, IEnumerable<VR>> mapper, string named = null)
            => FlatMapValues(new WrappedValueMapperWithKey<K, V, IEnumerable<VR>>(mapper), named);

        public IKStream<K, VR> FlatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper, string named = null)
            => FlatMapValues(WithKey(mapper), named);

        public IKStream<K, VR> FlatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper, string named = null)
        {
            if (mapper == null)
            {
                throw new ArgumentNullException($"Mapper function can't be null");
            }

            var name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.FLATMAPVALUES_NAME);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMapValues<K, V, VR>(mapper), name);
            ProcessorGraphNode<K, V> flatMapValuesNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            flatMapValuesNode.ValueChangingOperation = true;

            builder.AddGraphNode(Node, flatMapValuesNode);

            // value serde cannot be preserved
            return new KStream<K, VR>(
                name,
                KeySerdes,
                null,
                SetSourceNodes,
                flatMapValuesNode,
                builder);
        }

        #endregion

        #region Foreach

        public void Foreach(Action<K, V> action, string named = null)
        {
            if (action == null)
            {
                throw new ArgumentNullException("Foreach() doesn't allow null action function ");
            }

            String name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.FOREACH_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamPeek<K, V>(action, false), name);
            ProcessorGraphNode<K, V> foreachNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.AddGraphNode(Node, foreachNode);
        }

        #endregion

        #region Peek

        public IKStream<K, V> Peek(Action<K, V> action, string named = null)
        {
            if (action == null)
            {
                throw new ArgumentNullException(nameof(action));
            }

            String name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.PEEK_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamPeek<K, V>(action, true), name);
            ProcessorGraphNode<K, V> peekNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.AddGraphNode(Node, peekNode);

            return new KStream<K, V>(
                name,
                KeySerdes,
                ValueSerdes,
                SetSourceNodes,
                peekNode,
                builder);
        }

        #endregion

        #region Map

        public IKStream<KR, VR> Map<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> mapper, string named = null)
            => Map(new WrappedKeyValueMapper<K, V, KeyValuePair<KR, VR>>(mapper), named);

        public IKStream<KR, VR> Map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper, string named = null)
        {
            if (mapper == null)
            {
                throw new ArgumentNullException($"Map() doesn't allow null mapper function");
            }

            string name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.MAP_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamMap<K, V, KR, VR>(mapper), name);
            ProcessorGraphNode<K, V> mapProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            mapProcessorNode.KeyChangingOperation = true;

            builder.AddGraphNode(Node, mapProcessorNode);

            // key and value serde cannot be preserved
            return new KStream<KR, VR>(
                    name,
                    null,
                    null,
                    SetSourceNodes,
                    mapProcessorNode,
                    builder);
        }

        #endregion

        #region MapValues

        public IKStream<K, VR> MapValues<VR>(Func<V, VR> mapper, string named = null)
            => MapValues(new WrappedValueMapper<V, VR>(mapper), named);

        public IKStream<K, VR> MapValues<VR>(Func<K, V, VR> mapper, string named = null)
            => MapValues(new WrappedValueMapperWithKey<K, V, VR>(mapper), named);

        public IKStream<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, string named = null)
            => MapValues(WithKey(mapper), named);

        public IKStream<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, string named = null)
        {
            if (mapper == null)
            {
                throw new ArgumentNullException($"Mapper function can't be null");
            }

            String name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.MAPVALUES_NAME);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamMapValues<K, V, VR>(mapper), name);
            ProcessorGraphNode<K, V> mapValuesProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            mapValuesProcessorNode.ValueChangingOperation = true;

            builder.AddGraphNode(Node, mapValuesProcessorNode);

            // value serde cannot be preserved
            return new KStream<K, VR>(
                    name,
                    KeySerdes,
                    null,
                    SetSourceNodes,
                    mapValuesProcessorNode,
                    builder);
        }

        #endregion

        #region Print

        public void Print(Printed<K, V> printed)
        {
            if (printed == null)
            {
                throw new ArgumentNullException("Print() doesn't allow null printed instance");
            }

            var name = new Named(printed.Name).OrElseGenerateWithPrefix(builder, KStream.PRINTING_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(printed.Build(NameNode), name);
            ProcessorGraphNode<K, V> printNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.AddGraphNode(Node, printNode);
        }

        #endregion

        #region SelectKey

        public IKStream<KR, V> SelectKey<KR>(Func<K, V, KR> mapper, string named = null)
            => SelectKey(new WrappedKeyValueMapper<K, V, KR>(mapper), named);

        public IKStream<KR, V> SelectKey<KR>(IKeyValueMapper<K, V, KR> mapper, string named = null)
        {
            if (mapper == null)
            {
                throw new ArgumentNullException("SelectKey() doesn't allow null mapper function");
            }

            ProcessorGraphNode<K, V> selectKeyProcessorNode = InternalSelectKey(mapper, named);
            selectKeyProcessorNode.KeyChangingOperation = true;

            builder.AddGraphNode(Node, selectKeyProcessorNode);

            // key serde cannot be preserved
            return new KStream<KR, V>(
                selectKeyProcessorNode.streamGraphNode,
                null,
                ValueSerdes,
                SetSourceNodes,
                selectKeyProcessorNode,
                builder);
        }

        #endregion

        #region GroupBy

        public IKGroupedStream<KR, V> GroupBy<KR>(IKeyValueMapper<K, V, KR> keySelector, string named = null)
            => DoGroup(keySelector, Grouped<KR, V>.Create(named, null, ValueSerdes));

        public IKGroupedStream<KR, V> GroupBy<KR>(Func<K, V, KR> keySelector, string named = null)
            => GroupBy(new WrappedKeyValueMapper<K, V, KR>(keySelector), named);

        public IKGroupedStream<KR, V> GroupBy<KR, KS>(IKeyValueMapper<K, V, KR> keySelector, string named = null)
             where KS : ISerDes<KR>, new()
            => DoGroup(keySelector, Grouped<KR, V>.Create<KS>(named, ValueSerdes));

        public IKGroupedStream<KR, V> GroupBy<KR, KS>(Func<K, V, KR> keySelector, string named = null)
             where KS : ISerDes<KR>, new()
            => GroupBy<KR, KS>(new WrappedKeyValueMapper<K, V, KR>(keySelector), named);

        public IKGroupedStream<K, V> GroupByKey(string named = null)
        {
            return new KGroupedStream<K, V>(
                NameNode,
                Grouped<K, V>.Create(named, KeySerdes, ValueSerdes),
                SetSourceNodes,
                Node,
                builder);
        }

        public IKGroupedStream<K, V> GroupByKey<KS, VS>(string named = null)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            return new KGroupedStream<K, V>(
                NameNode,
                Grouped<K, V>.Create<KS, VS>(named),
                SetSourceNodes,
                Node,
                builder);
        }

        #endregion

        #region Join Table

        public IKStream<K, VR> Join<V0, VR, V0S>(IKTable<K, V0> table, Func<V, V0, VR> valueJoiner, string named = null)
            where V0S : ISerDes<V0>, new()
            => Join<V0, VR, V0S>(table, new WrappedValueJoiner<V, V0, VR>(valueJoiner), named);

        public IKStream<K, VR> Join<V0, VR>(IKTable<K, V0> table, Func<V, V0, VR> valueJoiner, string named = null)
            => Join(table, new WrappedValueJoiner<V, V0, VR>(valueJoiner), named);

        public IKStream<K, VR> Join<V0, VR>(IKTable<K, V0> table, IValueJoiner<V, V0, VR> valueJoiner, string named = null)
        {
            return table is AbstractStream<K, V0> ?
                Join(table, valueJoiner, ((AbstractStream<K, V0>)table).ValueSerdes, named) :
                Join(table, valueJoiner, null, named);
        }

        public IKStream<K, VR> Join<V0, VR, V0S>(IKTable<K, V0> table, IValueJoiner<V, V0, VR> valueJoiner, string named = null)
            where V0S : ISerDes<V0>, new()
            => Join(table, valueJoiner, new V0S(), named);

        private IKStream<K, VR> Join<V0, VR>(IKTable<K, V0> table, IValueJoiner<V, V0, VR> valueJoiner, ISerDes<V0> valueSerdes, string named = null)
        {
            var joined = new Joined<K, V, V0>(KeySerdes, ValueSerdes, valueSerdes, named);
            return DoStreamTableJoin(table, valueJoiner, joined, false);
        }


        #endregion

        #region LeftJoin Table

        public IKStream<K, VR> LeftJoin<VT, VR, VTS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, string named = null)
            where VTS : ISerDes<VT>, new()
            => LeftJoin<VT, VR, VTS>(table, new WrappedValueJoiner<V, VT, VR>(valueJoiner), named);

        public IKStream<K, VR> LeftJoin<VT, VR, VTS>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner, string named = null)
            where VTS : ISerDes<VT>, new()
            => LeftJoin(table, valueJoiner, new VTS(), named);

        public IKStream<K, VR> LeftJoin<VT, VR>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, string named = null)
            => LeftJoin(table, new WrappedValueJoiner<V, VT, VR>(valueJoiner), named);

        public IKStream<K, VR> LeftJoin<VT, VR>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner, string named = null)
        {
            return table is AbstractStream<K, VT> ?
                LeftJoin(table, valueJoiner, ((AbstractStream<K, VT>)table).ValueSerdes, named) :
                LeftJoin(table, valueJoiner, null, named);
        }

        private IKStream<K, VR> LeftJoin<VT, VR>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner, ISerDes<VT> valueSerdes, string named = null)
        {
            var joined = new Joined<K, V, VT>(KeySerdes, ValueSerdes, valueSerdes, named);
            return DoStreamTableJoin(table, valueJoiner, joined, true);
        }

        #endregion

        #region Join GlobalTable

        public IKStream<K, VR> Join<K0, V0, VR>(IGlobalKTable<K0, V0> globalTable, Func<K, V, K0> keyMapper, Func<V, V0, VR> valueJoiner, string named = null)
            => Join(globalTable, new WrappedKeyValueMapper<K, V, K0>(keyMapper), new WrappedValueJoiner<V, V0, VR>(valueJoiner), named);

        public IKStream<K, VR> Join<K0, V0, VR>(IGlobalKTable<K0, V0> globalTable, IKeyValueMapper<K, V, K0> keyMapper, IValueJoiner<V, V0, VR> valueJoiner, string named = null)
            => GlobalTableJoin(globalTable, keyMapper, valueJoiner, false, named);

        #endregion

        #region LeftJoin GlobalTable

        public IKStream<K, VR> LeftJoin<K0, V0, VR>(IGlobalKTable<K0, V0> globalTable, Func<K, V, K0> keyMapper, Func<V, V0, VR> valueJoiner, string named = null)
            => LeftJoin(globalTable, new WrappedKeyValueMapper<K, V, K0>(keyMapper), new WrappedValueJoiner<V, V0, VR>(valueJoiner), named);

        public IKStream<K, VR> LeftJoin<K0, V0, VR>(IGlobalKTable<K0, V0> globalTable, IKeyValueMapper<K, V, K0> keyMapper, IValueJoiner<V, V0, VR> valueJoiner, string named = null)
            => GlobalTableJoin(globalTable, keyMapper, valueJoiner, true, named);

        #endregion

        #region Join Stream

        public IKStream<K, VR> Join<V0, VR, V0S>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null)
            where V0S : ISerDes<V0>, new()
            => Join<V0, VR, V0S>(stream, new WrappedValueJoiner<V, V0, VR>(valueJoiner), windows, props);

        public IKStream<K, VR> Join<V0, VR>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null)
            => Join(stream, new WrappedValueJoiner<V, V0, VR>(valueJoiner), windows, props);

        public IKStream<K, VR> Join<V0, VR, V0S>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null)
            where V0S : ISerDes<V0>, new()
            => Join(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), new V0S());

        public IKStream<K, VR> Join<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null)
        {
            return stream is AbstractStream<K, V0> ?
                Join(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), ((AbstractStream<K, V0>)stream).ValueSerdes) :
                Join(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), null);
        }

        private IKStream<K, VR> Join<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props, ISerDes<V0> valueSerdes)
        {
            props = props ?? StreamJoinProps.From<K, V, V0>(null);
            props.RightValueSerdes = props.RightValueSerdes ?? valueSerdes;
            props.KeySerdes = props.KeySerdes ?? KeySerdes;
            props.LeftValueSerdes = props.LeftValueSerdes ?? ValueSerdes;

            return DoJoin(stream, valueJoiner, windows, props, new StreamJoinBuilder(builder, false, false));
        }

        #endregion

        #region LeftJoin Stream

        public IKStream<K, VR> LeftJoin<V0, VR, V0S>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null) where V0S : ISerDes<V0>, new()
            => LeftJoin<V0, VR, V0S>(stream, new WrappedValueJoiner<V, V0, VR>(valueJoiner), windows, props);

        public IKStream<K, VR> LeftJoin<V0, VR>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null)
            => LeftJoin(stream, new WrappedValueJoiner<V, V0, VR>(valueJoiner), windows, props);

        public IKStream<K, VR> LeftJoin<V0, VR, V0S>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null) where V0S : ISerDes<V0>, new()
            => LeftJoin(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), new V0S());

        public IKStream<K, VR> LeftJoin<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null)
        {
            return stream is AbstractStream<K, V0> ?
                LeftJoin(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), ((AbstractStream<K, V0>)stream).ValueSerdes) :
                LeftJoin(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), null);
        }

        private IKStream<K, VR> LeftJoin<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props, ISerDes<V0> valueSerdes)
        {
            props = props ?? StreamJoinProps.From<K, V, V0>(null);
            props.RightValueSerdes = valueSerdes;
            if (props.KeySerdes == null)
            {
                props.KeySerdes = KeySerdes;
            }

            if (props.LeftValueSerdes == null)
            {
                props.LeftValueSerdes = ValueSerdes;
            }

            return DoJoin(stream, valueJoiner, windows, props, new StreamJoinBuilder(builder, true, false));
        }

        #endregion

        #region OuterJoin Stream

        public IKStream<K, VR> OuterJoin<V0, VR, V0S>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null) where V0S : ISerDes<V0>, new()
            => OuterJoin<V0, VR, V0S>(stream, new WrappedValueJoiner<V, V0, VR>(valueJoiner), windows, props);


        public IKStream<K, VR> OuterJoin<V0, VR>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null)
            => OuterJoin(stream, new WrappedValueJoiner<V, V0, VR>(valueJoiner), windows, props);


        public IKStream<K, VR> OuterJoin<V0, VR, V0S>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null) where V0S : ISerDes<V0>, new()
            => OuterJoin(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), new V0S());


        public IKStream<K, VR> OuterJoin<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null)
        {
            return stream is AbstractStream<K, V0> ?
                OuterJoin(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), ((AbstractStream<K, V0>)stream).ValueSerdes) :
                OuterJoin(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), null);
        }

        private IKStream<K, VR> OuterJoin<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props, ISerDes<V0> valueSerdes)
        {
            props = props ?? StreamJoinProps.From<K, V, V0>(null);
            props.RightValueSerdes = valueSerdes;
            if (props.KeySerdes == null)
            {
                props.KeySerdes = KeySerdes;
            }

            if (props.LeftValueSerdes == null)
            {
                props.LeftValueSerdes = ValueSerdes;
            }

            return DoJoin(stream, valueJoiner, windows, props, new StreamJoinBuilder(builder, true, true));
        }

        #endregion

        #region ToTable

        public IKTable<K, V> ToTable()
            => ToTable(null);

        public IKTable<K, V> ToTable(Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            materialized = materialized ?? Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.Create();

            var name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.TO_KTABLE_NAME);
            materialized.UseProvider(builder, KStream.TO_KTABLE_NAME);

            ISerDes<K> keySerdesOv = materialized.KeySerdes == null ? KeySerdes : materialized.KeySerdes;
            ISerDes<V> valueSerdesOv = materialized.ValueSerdes == null ? ValueSerdes : materialized.ValueSerdes;

            var tableSource = new KTableSource<K, V>(materialized.StoreName, materialized.QueryableStoreName);
            var parameters = new ProcessorParameters<K, V>(tableSource, name);

            var storeBuilder = new TimestampedKeyValueStoreMaterializer<K, V>(materialized).Materialize();
            var tableNode = new StatefulProcessorNode<K, V, ITimestampedKeyValueStore<K, V>>(name, parameters, storeBuilder);
            builder.AddGraphNode(Node, tableNode);

            return new KTable<K, V, V>(
                name,
                keySerdesOv,
                valueSerdesOv,
                SetSourceNodes,
                materialized.QueryableStoreName,
                tableSource,
                tableNode,
                builder);
        }

        #endregion

        #region Private

        private IKStream<K, V> DoFilter(Func<K, V, bool> predicate, string named, bool not)
        {
            if (predicate == null)
            {
                throw new ArgumentNullException($"Filter() doesn't allow null predicate function");
            }

            string name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.FILTER_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(predicate, not), name);
            ProcessorGraphNode<K, V> filterProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.AddGraphNode(Node, filterProcessorNode);
            return new KStream<K, V>(name, KeySerdes, ValueSerdes, SetSourceNodes, filterProcessorNode, builder);
        }

        private void DoTo(ITopicNameExtractor<K, V> topicExtractor, Produced<K, V> produced)
        {
            string name = new Named(produced.Named).OrElseGenerateWithPrefix(builder, KStream.SINK_NAME);

            StreamSinkNode<K, V> sinkNode = new StreamSinkNode<K, V>(topicExtractor, name, produced);
            builder.AddGraphNode(Node, sinkNode);
        }

        private IKStream<K, V>[] DoBranch(string named = null, params Func<K, V, bool>[] predicates)
        {
            var namedInternal = new Named(named);
            if (predicates != null && predicates.Length == 0)
            {
                throw new ArgumentException("Branch() requires at least one predicate");
            }

            if (predicates == null || predicates.Any(p => p == null))
            {
                throw new ArgumentNullException("Branch() doesn't allow null predicate function");
            }

            String branchName = namedInternal.OrElseGenerateWithPrefix(builder, KStream.BRANCH_NAME);
            String[] childNames = new String[predicates.Length];
            for (int i = 0; i < predicates.Length; i++)
            {
                childNames[i] = namedInternal.SuffixWithOrElseGet($"predicate-{i}", builder, KStream.BRANCHCHILD_NAME);
            }

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamBranch<K, V>(predicates, childNames), branchName);
            ProcessorGraphNode<K, V> branchNode = new ProcessorGraphNode<K, V>(branchName, processorParameters);

            builder.AddGraphNode(Node, branchNode);

            IKStream<K, V>[] branchChildren = new IKStream<K, V>[predicates.Length];
            for (int i = 0; i < predicates.Length; i++)
            {
                ProcessorParameters<K, V> innerProcessorParameters = new ProcessorParameters<K, V>(new PassThrough<K, V>(), childNames[i]);
                ProcessorGraphNode<K, V> branchChildNode = new ProcessorGraphNode<K, V>(childNames[i], innerProcessorParameters);

                builder.AddGraphNode(branchNode, branchChildNode);
                branchChildren[i] = new KStream<K, V>(childNames[i], KeySerdes, ValueSerdes, SetSourceNodes, branchChildNode, builder);
            }

            return branchChildren;
        }

        private IKGroupedStream<KR, V> DoGroup<KR>(IKeyValueMapper<K, V, KR> keySelector, Grouped<KR, V> grouped)
        {
            if (keySelector == null)
            {
                throw new ArgumentNullException("GroupBy() doesn't allow null selector function");
            }

            ProcessorGraphNode<K, V> selectKeyMapNode = InternalSelectKey(keySelector, grouped.Named);
            selectKeyMapNode.KeyChangingOperation = true;

            builder.AddGraphNode(Node, selectKeyMapNode);

            return new KGroupedStream<KR, V>(
                selectKeyMapNode.streamGraphNode,
                grouped,
                SetSourceNodes,
                selectKeyMapNode,
                builder);
        }

        private ProcessorGraphNode<K, V> InternalSelectKey<KR>(IKeyValueMapper<K, V, KR> mapper, string named = null)
        {
            var name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.KEY_SELECT_NAME);

            WrappedKeyValueMapper<K, V, KeyValuePair<KR, V>> internalMapper =
                new WrappedKeyValueMapper<K, V, KeyValuePair<KR, V>>(
                (key, value) => new KeyValuePair<KR, V>(mapper.Apply(key, value), value));

            KStreamMap<K, V, KR, V> kStreamMap = new KStreamMap<K, V, KR, V>(internalMapper);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(kStreamMap, name);

            return new ProcessorGraphNode<K, V>(name, processorParameters);
        }

        private KStream<K, VR> DoStreamTableJoin<V0, VR>(IKTable<K, V0> table, IValueJoiner<V, V0, VR> valueJoiner, Joined<K, V, V0> joined, bool leftJoin)
        {
            var allSourceNodes = EnsureJoinableWith((AbstractStream<K, V0>)table);

            string name = new Named(joined.Name).OrElseGenerateWithPrefix(builder, leftJoin ? KStream.LEFTJOIN_NAME : KStream.JOIN_NAME);
            var processorSupplier = new KStreamKTableJoin<K, VR, V, V0>(
                                        (table as IKTableGetter<K, V0>).ValueGetterSupplier,
                                        valueJoiner,
                                        leftJoin);
            var processorParameters = new ProcessorParameters<K, V>(processorSupplier, name);
            var streamTableJoinNode = new StreamTableJoinNode<K, V>(
                name,
                processorParameters,
                (table as IKTableGetter<K, V0>).ValueGetterSupplier.StoreNames,
                NameNode
            );

            builder.AddGraphNode(Node, streamTableJoinNode);
            return new KStream<K, VR>(
                name,
                joined.KeySerdes != null ? joined.KeySerdes : KeySerdes,
                null,
                allSourceNodes.ToList(),
                streamTableJoinNode,
                builder);
        }

        private KStream<K, VR> GlobalTableJoin<K0, V0, VR>(IGlobalKTable<K0, V0> globalTable, IKeyValueMapper<K, V, K0> keyMapper, IValueJoiner<V, V0, VR> valueJoiner, bool leftJoin, string named)
        {
            var supplier = (globalTable as GlobalKTable<K0, V0>).ValueGetterSupplier;
            var name = new Named(named).OrElseGenerateWithPrefix(builder, leftJoin ? KStream.LEFTJOIN_NAME : KStream.JOIN_NAME);

            var processorSupplier = new KStreamGlobalKTableJoin<K, K0, V, V0, VR>(
                supplier,
                valueJoiner,
                keyMapper,
                leftJoin);
            var parameters = new ProcessorParameters<K, V>(processorSupplier, name);
            var joinNode = new StreamTableJoinNode<K, V>(name, parameters, new string[0], null);

            builder.AddGraphNode(Node, joinNode);

            return new KStream<K, VR>(name, KeySerdes, null, SetSourceNodes, joinNode, builder);
        }

        private KStream<K, VR> DoJoin<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> joinedProps, StreamJoinBuilder builder)
        {
            CheckIfParamNull(stream, "stream");
            CheckIfParamNull(valueJoiner, "valueJoiner");

            KStream<K, V0> joinOther = (KStream<K, V0>)stream;

            return builder.Join(this, joinOther, valueJoiner, windows, joinedProps);
        }

        #endregion

    }
}