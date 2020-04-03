using kafka_stream_core.Processors;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.SerDes;
using kafka_stream_core.Stream.Internal.Graph;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Stream.Internal
{
    internal class KStream<K, V> : AbstractStream<K, V>, IKStream<K, V>
    {
        #region Constants

        internal static String JOINTHIS_NAME = "KSTREAM-JOINTHIS-";

        internal static String JOINOTHER_NAME = "KSTREAM-JOINOTHER-";

        internal static String JOIN_NAME = "KSTREAM-JOIN-";

        internal static String LEFTJOIN_NAME = "KSTREAM-LEFTJOIN-";

        internal static String MERGE_NAME = "KSTREAM-MERGE-";

        internal static String OUTERTHIS_NAME = "KSTREAM-OUTERTHIS-";

        internal static String OUTEROTHER_NAME = "KSTREAM-OUTEROTHER-";

        internal static String WINDOWED_NAME = "KSTREAM-WINDOWED-";

        internal static String SOURCE_NAME = "KSTREAM-SOURCE-";

        internal static String SINK_NAME = "KSTREAM-SINK-";

        internal static String REPARTITION_TOPIC_SUFFIX = "-repartition";

        internal static String BRANCH_NAME = "KSTREAM-BRANCH-";

        internal static String BRANCHCHILD_NAME = "KSTREAM-BRANCHCHILD-";

        internal static String FILTER_NAME = "KSTREAM-FILTER-";

        internal static String PEEK_NAME = "KSTREAM-PEEK-";

        internal static String FLATMAP_NAME = "KSTREAM-FLATMAP-";

        internal static String FLATMAPVALUES_NAME = "KSTREAM-FLATMAPVALUES-";

        internal static String MAP_NAME = "KSTREAM-MAP-";

        internal static String MAPVALUES_NAME = "KSTREAM-MAPVALUES-";

        internal static String PROCESSOR_NAME = "KSTREAM-PROCESSOR-";

        internal static String PRINTING_NAME = "KSTREAM-PRINTER-";

        internal static String KEY_SELECT_NAME = "KSTREAM-KEY-SELECT-";

        internal static String TRANSFORM_NAME = "KSTREAM-TRANSFORM-";

        internal static String TRANSFORMVALUES_NAME = "KSTREAM-TRANSFORMVALUES-";

        internal static String FOREACH_NAME = "KSTREAM-FOREACH-";

        #endregion

        internal KStream(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> setSourceNodes, StreamGraphNode node, InternalStreamBuilder builder)
            : base(name, keySerdes, valueSerdes, setSourceNodes, node, builder)
        {
        }

        #region Branch

        public IKStream<K, V>[] Branch(params Func<K, V, bool>[] predicates) => DoBranch(string.Empty, predicates);

        public IKStream<K, V>[] Branch(string named, params Func<K, V, bool>[] predicates) => DoBranch(named, predicates);

        #endregion

        #region Filter

        public IKStream<K, V> Filter(Func<K, V, bool> predicate, string named)
        {
            string name = this.builder.NewProcessorName(FILTER_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(predicate), name);
            ProcessorGraphNode<K, V> filterProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            this.builder.AddGraphNode(node, filterProcessorNode);
            return new KStream<K, V>(name, this.keySerdes, this.valueSerdes, this.setSourceNodes, filterProcessorNode, this.builder);

        }

        public IKStream<K, V> FilterNot(Func<K, V, bool> predicate, string named)
        {
            string name = this.builder.NewProcessorName(FILTER_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(predicate, true), name);
            ProcessorGraphNode<K, V> filterProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            this.builder.AddGraphNode(node, filterProcessorNode);
            return new KStream<K, V>(name, this.keySerdes, this.valueSerdes, this.setSourceNodes, filterProcessorNode, this.builder);

        }

        public IKStream<K, V> Filter(Func<K, V, bool> predicate)
            => this.Filter(predicate, string.Empty);

        public IKStream<K, V> FilterNot(Func<K, V, bool> predicate)
            => this.FilterNot(predicate, string.Empty);

        #endregion

        #region Transform

        #endregion

        #region To

        public void To(string topicName) => To(new StaticTopicNameExtractor<K, V>(topicName));

        public void To(ITopicNameExtractor<K, V> topicExtractor) => DoTo(topicExtractor, Produced<K, V>.Create(keySerdes, valueSerdes));

        public void To(Func<K, V, IRecordContext, string> topicExtractor) => To(new WrapperTopicNameExtractor<K, V>(topicExtractor));

        public void To<KS, VS>(Func<K, V, IRecordContext, string> topicExtractor)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => To<KS, VS>(new WrapperTopicNameExtractor<K, V>(topicExtractor));

        public void To<KS, VS>(string topicName)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => To<KS, VS>(new StaticTopicNameExtractor<K, V>(topicName));

        public void To<KS, VS>(ITopicNameExtractor<K, V> topicExtractor)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => DoTo(topicExtractor, Produced<K, V>.Create<KS, VS>());

        #endregion

        #region FlatMap

        public IKStream<KR, VR> FlatMap<KR, VR>(Func<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper)
            => this.FlatMap(mapper, string.Empty);

        public IKStream<KR, VR> FlatMap<KR, VR>(Func<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named)
            => this.FlatMap(new WrappedKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>>(mapper), named);

        public IKStream<KR, VR> FlatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper)
            => this.FlatMap(mapper, string.Empty);

        public IKStream<KR, VR> FlatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named)
        {
            var name = this.builder.NewProcessorName(FLATMAP_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMap<K, V, KR, VR>(mapper), name);
            ProcessorGraphNode<K, V> flatMapNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            flatMapNode.KeyChangingOperation = true;

            builder.AddGraphNode(node, flatMapNode);

            // key and value serde cannot be preserved
            return new KStream<KR, VR>(name, null, null, setSourceNodes, flatMapNode, builder);
        }

        #endregion

        #region FlatMapValues

        public IKStream<K, VR> FlatMapValues<VR>(Func<V, IEnumerable<VR>> mapper)
            => this.FlatMapValues(mapper, string.Empty);

        public IKStream<K, VR> FlatMapValues<VR>(Func<V, IEnumerable<VR>> mapper, string named)
            => this.FlatMapValues(new WrappedValueMapper<V, IEnumerable<VR>>(mapper), named);

        public IKStream<K, VR> FlatMapValues<VR>(Func<K, V, IEnumerable<VR>> mapper)
            => this.FlatMapValues(mapper, string.Empty);

        public IKStream<K, VR> FlatMapValues<VR>(Func<K, V, IEnumerable<VR>> mapper, string named)
            => this.FlatMapValues(new WrapperValueMapperWithKey<K, V, IEnumerable<VR>>(mapper), named);

        public IKStream<K, VR> FlatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper)
            => this.FlatMapValues(mapper, null);

        public IKStream<K, VR> FlatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper, string named)
            => this.FlatMapValues(WithKey(mapper), named);

        public IKStream<K, VR> FlatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
            => this.FlatMapValues(mapper, null);

        public IKStream<K, VR> FlatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper, string named)
        {
             var name = this.builder.NewProcessorName(FLATMAPVALUES_NAME);

            ProcessorParameters<K,V> processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMapValues<K, V, VR>(mapper), name);
            ProcessorGraphNode<K, V> flatMapValuesNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            flatMapValuesNode.ValueChangingOperation = true;

            builder.AddGraphNode(this.node, flatMapValuesNode);

            // value serde cannot be preserved
            return new KStream<K, VR>(
                name,
                this.keySerdes,
                null,
                this.setSourceNodes,
                flatMapValuesNode,
                builder);
        }

        #endregion

        #region Foreach

        public void Foreach(Action<K, V> action) => this.Foreach(action, string.Empty);

        public void Foreach(Action<K, V> action, string named)
        {
            String name = this.builder.NewProcessorName(FOREACH_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamPeek<K, V>(action, false), name);
            ProcessorGraphNode<K, V> foreachNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            this.builder.AddGraphNode(node, foreachNode);
        }

        #endregion

        #region Peek

        public IKStream<K, V> Peek(Action<K, V> action) => this.Peek(action, string.Empty);

        public IKStream<K, V> Peek(Action<K, V> action, string named)
        {
            String name = this.builder.NewProcessorName(PEEK_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamPeek<K, V>(action, true), name);
            ProcessorGraphNode<K, V> peekNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.AddGraphNode(node, peekNode);

            return new KStream<K, V>(
                name,
                keySerdes,
                valueSerdes,
                setSourceNodes,
                peekNode,
                builder);
        }

        #endregion

        #region Map

        public IKStream<KR, VR> Map<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> mapper)
            => this.Map(mapper, string.Empty);

        public IKStream<KR, VR> Map<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> mapper, string named)
            => this.Map(new WrappedKeyValueMapper<K, V, KeyValuePair<KR, VR>>(mapper), named);

        public IKStream<KR, VR> Map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper)
            => this.Map(mapper, string.Empty);

        public IKStream<KR, VR> Map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper, string named)
        {
            string name = this.builder.NewProcessorName(MAP_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamMap<K, V, KR, VR>(mapper), name);
            ProcessorGraphNode<K, V> mapProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            mapProcessorNode.KeyChangingOperation = true;

            builder.AddGraphNode(node, mapProcessorNode);

            // key and value serde cannot be preserved
            return new KStream<KR, VR>(
                    name,
                    null,
                    null,
                    setSourceNodes,
                    mapProcessorNode,
                    builder);
        }

        #endregion

        #region MapValues

        public IKStream<K, VR> MapValues<VR>(Func<V, VR> mapper)
            => this.MapValues(mapper, string.Empty);

        public IKStream<K, VR> MapValues<VR>(Func<V, VR> mapper, string named)
            => this.MapValues(new WrappedValueMapper<V, VR>(mapper), named);

        public IKStream<K, VR> MapValues<VR>(Func<K, V, VR> mapper)
            => this.MapValues(mapper, string.Empty);

        public IKStream<K, VR> MapValues<VR>(Func<K, V, VR> mapper, string named)
            => this.MapValues(new WrapperValueMapperWithKey<K, V, VR>(mapper), named);

        public IKStream<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper)
            => this.MapValues<VR>(mapper, null);

        public IKStream<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, string named)
            => this.MapValues<VR>(WithKey(mapper), named);

        public IKStream<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapper)
            => this.MapValues<VR>(mapper, null);

        public IKStream<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, string named)
        {
            String name = this.builder.NewProcessorName(MAPVALUES_NAME);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamMapValues<K, V, VR>(mapper), name);
            ProcessorGraphNode<K, V> mapValuesProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            mapValuesProcessorNode.ValueChangingOperation = true;

            builder.AddGraphNode(this.node, mapValuesProcessorNode);

            // value serde cannot be preserved
            return new KStream<K, VR>(
                    name,
                    this.keySerdes,
                    null,
                    this.setSourceNodes,
                    mapValuesProcessorNode,
                    builder);
        }

        #endregion

        #region Print

        public void Print(Printed<K, V> printed)
        {
            var name = this.builder.NewProcessorName(PRINTING_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(printed.Build(this.nameNode), name);
            ProcessorGraphNode<K, V> printNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.AddGraphNode(node, printNode);
        }

        #endregion

        #region SelectKey

        public IKStream<KR, V> SelectKey<KR>(Func<K, V, KR> mapper)
            => this.SelectKey(mapper, string.Empty);

        public IKStream<KR, V> SelectKey<KR>(Func<K, V, KR> mapper, string named)
            => this.SelectKey(new WrappedKeyValueMapper<K, V, KR>(mapper), named);

        public IKStream<KR, V> SelectKey<KR>(IKeyValueMapper<K, V, KR> mapper)
            => this.SelectKey(mapper, string.Empty);

        public IKStream<KR, V> SelectKey<KR>(IKeyValueMapper<K, V, KR> mapper, string named)
        {
            ProcessorGraphNode<K, V> selectKeyProcessorNode = InternalSelectKey(mapper, named);
            selectKeyProcessorNode.KeyChangingOperation = true;

            builder.AddGraphNode(node, selectKeyProcessorNode);

            // key serde cannot be preserved
            return new KStream<KR, V>(
                selectKeyProcessorNode.streamGraphNode,
                null,
                valueSerdes,
                setSourceNodes,
                selectKeyProcessorNode,
                builder);
        }

        #endregion

        #region GroupBy

        public IKGroupedStream<KR, V> GroupBy<KR>(IKeyValueMapper<K, V, KR> keySelector)
            => DoGroup(keySelector, Grouped<KR, V>.Create(null, valueSerdes));

        public IKGroupedStream<KR, V> GroupBy<KR>(Func<K, V, KR> keySelector)
            => this.GroupBy(new WrappedKeyValueMapper<K, V, KR>(keySelector));

        public IKGroupedStream<KR, V> GroupBy<KR, KS>(IKeyValueMapper<K, V, KR> keySelector)
             where KS : ISerDes<KR>, new()
            => DoGroup(keySelector, Grouped<KR, V>.Create<KS>(valueSerdes));

        public IKGroupedStream<KR, V> GroupBy<KR, KS>(Func<K, V, KR> keySelector)
             where KS : ISerDes<KR>, new()
            => this.GroupBy<KR, KS>(new WrappedKeyValueMapper<K, V, KR>(keySelector));

        public IKGroupedStream<K, V> GroupByKey()
        {
            return new KGroupedStream<K, V>(
                this.nameNode,
                Grouped<K, V>.Create(this.keySerdes, this.valueSerdes),
                this.setSourceNodes,
                this.node,
                builder);
        }

        public IKGroupedStream<K, V> GroupByKey<KS, VS>()
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
        {
            return new KGroupedStream<K, V>(
                this.nameNode,
                Grouped<K, V>.Create<KS, VS>(null),
                this.setSourceNodes,
                this.node,
                builder);
        }

        #endregion

        #region Private

        private void DoTo(ITopicNameExtractor<K, V> topicExtractor, Produced<K, V> produced)
        {
            string name = this.builder.NewProcessorName(SINK_NAME);

            StreamSinkNode<K, V> sinkNode = new StreamSinkNode<K, V>(topicExtractor, name, produced);
            this.builder.AddGraphNode(node, sinkNode);
        }

        private IKStream<K, V>[] DoBranch(string named, params Func<K, V, bool>[] predicates)
        {
            if (predicates.Length == 0)
                throw new ArgumentException("branch() requires at least one predicate");

            String branchName = this.builder.NewProcessorName(BRANCH_NAME);
            String[] childNames = new String[predicates.Length];
            for (int i = 0; i < predicates.Length; i++)
                childNames[i] = $"{this.builder.NewProcessorName(BRANCHCHILD_NAME)}-predicate-{i}";

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamBranch<K, V>(predicates, childNames), branchName);
            ProcessorGraphNode<K, V> branchNode = new ProcessorGraphNode<K, V>(branchName, processorParameters);

            this.builder.AddGraphNode(node, branchNode);

            IKStream<K, V>[] branchChildren = new IKStream<K, V>[predicates.Length];
            for (int i = 0; i < predicates.Length; i++)
            {
                ProcessorParameters<K, V> innerProcessorParameters = new ProcessorParameters<K, V>(new PassThrough<K, V>(), childNames[i]);
                ProcessorGraphNode<K, V> branchChildNode = new ProcessorGraphNode<K, V>(childNames[i], innerProcessorParameters);

                builder.AddGraphNode(branchNode, branchChildNode);
                branchChildren[i] = new KStream<K, V>(childNames[i], this.keySerdes, this.valueSerdes, setSourceNodes, branchChildNode, builder);
            }

            return branchChildren;
        }

        private IKGroupedStream<KR, V> DoGroup<KR>(IKeyValueMapper<K, V, KR> keySelector, Grouped<KR, V> grouped)
        {
            ProcessorGraphNode<K, V> selectKeyMapNode = InternalSelectKey(keySelector, grouped.Named);
            selectKeyMapNode.KeyChangingOperation = true;

            builder.AddGraphNode(this.node, selectKeyMapNode);

            return new KGroupedStream<KR, V>(
                selectKeyMapNode.streamGraphNode,
                grouped,
                this.setSourceNodes,
                selectKeyMapNode,
                builder);
        }

        private ProcessorGraphNode<K, V> InternalSelectKey<KR>(IKeyValueMapper<K, V, KR> mapper, string named)
        {
            var name = this.builder.NewProcessorName(KEY_SELECT_NAME);

            WrappedKeyValueMapper<K, V, KeyValuePair<KR, V>> internalMapper = 
                new WrappedKeyValueMapper<K, V, KeyValuePair<KR, V>>(
                (key, value) => new KeyValuePair<KR, V>(mapper.Apply(key, value), value));

            KStreamMap <K, V, KR, V> kStreamMap = new KStreamMap<K, V, KR, V>(internalMapper);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(kStreamMap, name);

            return new ProcessorGraphNode<K, V>(name, processorParameters);
        }

        #endregion
    }
}