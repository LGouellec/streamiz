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
using System.Threading.Tasks;
using Streamiz.Kafka.Net.Processors.Public;

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
        internal static readonly string REPARTITION_NAME = "KSTREAM-REPARTITION-";
        internal static readonly string MAP_ASYNC_NAME = "KSTREAM-MAP-ASYNC-";
        internal static readonly string MAPVALUES_ASYNC_NAME = "KSTREAM-MAPVALUES-ASYNC-";
        internal static readonly string FLATMAP_ASYNC_NAME = "KSTREAM-FLATMAP-ASYNC-";
        internal static readonly string FLATMAPVALUES_ASYNC_NAME = "KSTREAM-FLATMAPVALUES-ASYNC-";
        internal static readonly string FOREACH_ASYNC_NAME = "KSTREAM-FOREACH-ASYNC-";
        internal static readonly string RECORD_TIMESTAMP_NAME = "KSTREAM-RECORDTIMESTAMP-";

        internal static readonly string REQUEST_SINK_SUFFIX = "-request-sink";
        internal static readonly string RESPONSE_SINK_SUFFIX = "-response-sink";
        internal static readonly string REQUEST_SOURCE_SUFFIX = "-request-source";
        internal static readonly string RESPONSE_SOURCE_SUFFIX = "-response-source";

        #endregion
    }
    
    internal class KStream<K, V> : AbstractStream<K, V>, IKStream<K, V>
    {
        private RepartitionNode<K, V> RepartitionNode { get; set; }
        
        internal KStream(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> setSourceNodes, StreamGraphNode node, InternalStreamBuilder builder)
            : base(name, keySerdes, valueSerdes, setSourceNodes, node, builder)
        { }

        private KStream(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> setSourceNodes, bool repartitionRequired, StreamGraphNode node, InternalStreamBuilder builder)
            : base(name, keySerdes, valueSerdes, setSourceNodes, repartitionRequired, node, builder)
        { }

        #region Branch

        public IKStream<K, V>[] Branch(params Func<K, V, bool>[] predicates) => DoBranch(string.Empty, predicates);

        public IKStream<K, V>[] Branch(string named, params Func<K, V, bool>[] predicates) => DoBranch(named, predicates);

        #endregion

        #region Merge

        public IKStream<K, V> Merge(IKStream<K, V> stream, string named = null)
        {
            return DoMerge(stream, named);
        } 

        #endregion

        #region Filter

        public IKStream<K, V> Filter(Func<K, V, bool> predicate, string named = null)
            => DoFilter(predicate, named, false);

        public IKStream<K, V> FilterNot(Func<K, V, bool> predicate, string named = null)
            => DoFilter(predicate, named, true);

        #endregion
        
        #region Process

        public void Process(ProcessorSupplier<K, V> processorSupplier, string named = null, params string[] storeNames)
        {
            string name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.PROCESSOR_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(
                new KStreamProcessorSupplier<K, V>(processorSupplier), name);
            StatefulProcessorNode<K, V> processorNode = new StatefulProcessorNode<K, V>(name, processorParameters, processorSupplier.StoreBuilder, storeNames);

            builder.AddGraphNode(Node, processorNode);
        }
        
        #endregion

        #region Transform

        public IKStream<K1, V1> Transform<K1, V1>(TransformerSupplier<K, V, K1, V1> transformerSupplier, string named = null, params string[] storeNames)
            => DoTransform(transformerSupplier, true, named, storeNames);

        public IKStream<K, V1> TransformValues<V1>(TransformerSupplier<K, V, K, V1> transformerSupplier, string named = null, params string[] storeNames)
            => DoTransform(transformerSupplier, false, named, storeNames);
        
        private IKStream<K1, V1> DoTransform<K1, V1>(TransformerSupplier<K, V, K1, V1> transformerSupplier, bool changeKey, string named =
            null, params string[] storeNames)
        {
            string name = new Named(named).OrElseGenerateWithPrefix(builder, changeKey ? KStream.TRANSFORM_NAME : KStream.TRANSFORMVALUES_NAME );
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(
                new KStreamTransformerSupplier<K, V, K1, V1>(transformerSupplier, changeKey), name);
            StatefulProcessorNode<K, V> processorNode = new StatefulProcessorNode<K, V>(name, processorParameters, transformerSupplier.StoreBuilder, storeNames);

            builder.AddGraphNode(Node, processorNode);
            
            return new KStream<K1, V1>(
                name,
                null,
                null,
                SetSourceNodes,
                changeKey,
                processorNode,
                builder);
        }

        #endregion
        
        #region Repartition

        public IKStream<K, V> Repartition(Repartitioned<K, V> repartitioned = null)
        {
            repartitioned ??= Repartitioned<K, V>.Empty();
            string name =  repartitioned.Named ?? builder.NewProcessorName(KStream.REPARTITION_NAME);
            ISerDes<K> keySerdes = repartitioned.KeySerdes ?? KeySerdes;
            ISerDes<V> valueSerdes = repartitioned.ValueSerdes ?? ValueSerdes;

            (string repartitionName, RepartitionNode<K, V> repartitionNode) =
                CreateRepartitionSource(name, keySerdes, valueSerdes, builder);
                
            repartitionNode.NumberOfPartition = repartitioned.NumberOfPartition;
            repartitionNode.StreamPartitioner = repartitioned.StreamPartitioner;
            
            builder.AddGraphNode(Node, repartitionNode);
            
            return new KStream<K, V>(
                repartitionName,
                keySerdes,
                valueSerdes,
                name.ToSingle().ToList(),
                repartitionNode,
                builder);
        }
        
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

        public void To(string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, string named = null)
        {
            if (topicName == null)
            {
                throw new ArgumentNullException(nameof(topicName));
            }

            if (string.IsNullOrEmpty(topicName))
            {
                throw new ArgumentException("topicName must be empty");
            }

            To(new StaticTopicNameExtractor<K, V>(topicName), keySerdes, valueSerdes, named);
        }

        public void To(ITopicNameExtractor<K, V> topicExtractor, string named = null) => DoTo(topicExtractor, Produced<K, V>.Create(KeySerdes, ValueSerdes).WithName(named));

        public void To(ITopicNameExtractor<K, V> topicExtractor, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, string named = null)
            => DoTo(topicExtractor, Produced<K,V>.Create(keySerdes, valueSerdes).WithName(named));

        public void To(Func<K, V, IRecordContext, string> topicExtractor, string named = null) => To(new WrapperTopicNameExtractor<K, V>(topicExtractor), named);

        public void To(Func<K, V, IRecordContext, string> topicExtractor, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, string named = null)
            => To(new WrapperTopicNameExtractor<K, V>(topicExtractor), keySerdes, valueSerdes, named);

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
            return new KStream<KR, VR>(name, null, null, SetSourceNodes, true, flatMapNode, builder);
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
                RepartitionRequired,
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
                RepartitionRequired,
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
                    true,
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
                    RepartitionRequired,
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
                true,
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

        public IKGroupedStream<KR, V> GroupBy<KR, KRS, VS>(IKeyValueMapper<K, V, KR> keySelector, string named = null)
            where KRS : ISerDes<KR>, new()
            where VS : ISerDes<V>, new()
            => DoGroup(keySelector, Grouped<KR, V>.Create<KRS, VS>(named));

        public IKGroupedStream<KR, V> GroupBy<KR, KS>(Func<K, V, KR> keySelector, string named = null)
             where KS : ISerDes<KR>, new()
            => GroupBy<KR, KS>(new WrappedKeyValueMapper<K, V, KR>(keySelector), named);

        public IKGroupedStream<KR, V> GroupBy<KR, KRS, VS>(Func<K, V, KR> keySelector, string named = null)
            where KRS : ISerDes<KR>, new() 
            where VS : ISerDes<V>, new()
            => GroupBy<KR, KRS, VS>(new WrappedKeyValueMapper<K, V, KR>(keySelector), named);
        
        public IKGroupedStream<K, V> GroupByKey(string named = null)
        {
            return new KGroupedStream<K, V>(
                NameNode,
                Grouped<K, V>.Create(named, KeySerdes, ValueSerdes),
                SetSourceNodes,
                Node,
                RepartitionRequired,
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
                RepartitionRequired,
                builder);
        }

        #endregion

        #region Join Table

        public IKStream<K, VR> Join<V0, VR>(IKTable<K, V0> table, Func<V, V0, VR> valueJoiner,
            StreamTableJoinProps<K, V, V0> streamTableJoinProps, string named = null)
        {
            KeySerdes = streamTableJoinProps.KeySerdes;
            ValueSerdes = streamTableJoinProps.LeftValueSerdes;
            return Join(
                table,
      new WrappedValueJoiner<V, V0, VR>(valueJoiner),
                streamTableJoinProps.RightValueSerdes,
                null,
                named);
        }

        public IKStream<K, VR> Join<V0, VR>(IKTable<K, V0> table, IValueJoiner<V, V0, VR> valueJoiner,
            StreamTableJoinProps<K, V, V0> streamTableJoinProps, string named = null)
        {
            KeySerdes = streamTableJoinProps.KeySerdes;
            ValueSerdes = streamTableJoinProps.LeftValueSerdes;
            return Join(
                table,
                valueJoiner,
                streamTableJoinProps.RightValueSerdes,
                null,
                named);
        }

        public IKStream<K, VR> Join<V0, VR, V0S, VRS>(IKTable<K, V0> table, Func<V, V0, VR> valueJoiner, string named = null)
            where V0S : ISerDes<V0>, new()
            where VRS : ISerDes<VR>, new ()
            => Join<V0, VR, V0S, VRS>(table, new WrappedValueJoiner<V, V0, VR>(valueJoiner), named);

        public IKStream<K, VR> Join<V0, VR>(IKTable<K, V0> table, Func<V, V0, VR> valueJoiner, string named = null)
            => Join(table, new WrappedValueJoiner<V, V0, VR>(valueJoiner), named);

        public IKStream<K, VR> Join<V0, VR>(IKTable<K, V0> table, IValueJoiner<V, V0, VR> valueJoiner, string named = null)
        {
            return table is AbstractStream<K, V0> ?
                Join(table, valueJoiner, ((AbstractStream<K, V0>)table).ValueSerdes, null, named) :
                Join(table, valueJoiner, null, null, named);
        }

        public IKStream<K, VR> Join<V0, VR, V0S, VRS>(IKTable<K, V0> table, IValueJoiner<V, V0, VR> valueJoiner, string named = null)
            where V0S : ISerDes<V0>, new()
            where VRS : ISerDes<VR>, new ()
            => Join(table, valueJoiner, new V0S(), new VRS(), named);

        private IKStream<K, VR> Join<V0, VR>(IKTable<K, V0> table, IValueJoiner<V, V0, VR> valueJoiner, ISerDes<V0> valueSerdes, ISerDes<VR> otherValueSerdes, string named = null)
        {
            var joined = new Joined<K, V, V0>(KeySerdes, ValueSerdes, valueSerdes, named);

            if (RepartitionRequired)
            {
                var streamRepartitioned = RepartitionForJoin(named ?? NameNode, joined.KeySerdes, joined.ValueSerdes);
                return streamRepartitioned.DoStreamTableJoin(table, valueJoiner, joined, false, otherValueSerdes);
            }
            
            return DoStreamTableJoin(table, valueJoiner, joined, false, otherValueSerdes);
        }
        
        #endregion

        #region LeftJoin Table

        public IKStream<K, VR> LeftJoin<VT, VR>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner,
            StreamTableJoinProps<K, V, VT> streamTableJoinProps, string named = null)
        {
            KeySerdes = streamTableJoinProps.KeySerdes;
            ValueSerdes = streamTableJoinProps.LeftValueSerdes;
            return LeftJoin(
                table,
                new WrappedValueJoiner<V, VT, VR>(valueJoiner),
                streamTableJoinProps.RightValueSerdes,
                null,
                named);
        }

        public IKStream<K, VR> LeftJoin<VT, VR>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner,
            StreamTableJoinProps<K, V, VT> streamTableJoinProps, string named = null)
        {
            KeySerdes = streamTableJoinProps.KeySerdes;
            ValueSerdes = streamTableJoinProps.LeftValueSerdes;
            return LeftJoin(
                table,
                valueJoiner,
                streamTableJoinProps.RightValueSerdes,
                null,
                named);
        }
        
        public IKStream<K, VR> LeftJoin<VT, VR, VTS, VRS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, string named = null)
            where VTS : ISerDes<VT>, new()
            where VRS : ISerDes<VR>, new ()
            => LeftJoin<VT, VR, VTS, VRS>(table, new WrappedValueJoiner<V, VT, VR>(valueJoiner), named);

        public IKStream<K, VR> LeftJoin<VT, VR, VTS, VRS>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner, string named = null)
            where VTS : ISerDes<VT>, new()
            where VRS : ISerDes<VR>, new ()
            => LeftJoin(table, valueJoiner, new VTS(), new VRS(), named);

        public IKStream<K, VR> LeftJoin<VT, VR>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, string named = null)
            => LeftJoin(table, new WrappedValueJoiner<V, VT, VR>(valueJoiner), named);

        public IKStream<K, VR> LeftJoin<VT, VR>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner, string named = null)
        {
            return table is AbstractStream<K, VT> ?
                LeftJoin(table, valueJoiner, ((AbstractStream<K, VT>)table).ValueSerdes, null, named) :
                LeftJoin(table, valueJoiner, null, null, named);
        }

        private IKStream<K, VR> LeftJoin<VT, VR>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner, ISerDes<VT> valueSerdes, ISerDes<VR> otherValueSerdes, string named = null)
        {
            var joined = new Joined<K, V, VT>(KeySerdes, ValueSerdes, valueSerdes, named);
            
            if (RepartitionRequired)
            {
                var streamRepartitioned = RepartitionForJoin(named ?? NameNode, joined.KeySerdes, joined.ValueSerdes);
                return streamRepartitioned.DoStreamTableJoin(table, valueJoiner, joined, true, otherValueSerdes);
            }
            
            return DoStreamTableJoin(table, valueJoiner, joined, true, otherValueSerdes);
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

        public IKStream<K, VR> Join<V0, VR, V0S, VRS>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null)
            where V0S : ISerDes<V0>, new()
            where VRS : ISerDes<VR>, new ()
            => Join<V0, VR, V0S, VRS>(stream, new WrappedValueJoiner<V, V0, VR>(valueJoiner), windows, props);

        public IKStream<K, VR> Join<V0, VR>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null)
            => Join(stream, new WrappedValueJoiner<V, V0, VR>(valueJoiner), windows, props);

        public IKStream<K, VR> Join<V0, VR, V0S, VRS>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null)
            where V0S : ISerDes<V0>, new()
            where VRS : ISerDes<VR>, new ()
            => Join(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), new V0S(), new VRS());

        public IKStream<K, VR> Join<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null)
        {
            return stream is AbstractStream<K, V0> ?
                Join(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), ((AbstractStream<K, V0>)stream).ValueSerdes, null) :
                Join(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), null, null);
        }

        private IKStream<K, VR> Join<V0, VR>(
            IKStream<K, V0> stream,
            IValueJoiner<V, V0, VR> valueJoiner,
            JoinWindowOptions windows,
            StreamJoinProps<K, V, V0> props,
            ISerDes<V0> valueSerdes,
            ISerDes<VR> otherValueSerdes)
        {
            props ??= StreamJoinProps.From<K, V, V0>(null);
            props.RightValueSerdes ??= valueSerdes;
            props.KeySerdes ??= KeySerdes;
            props.LeftValueSerdes ??= ValueSerdes;

            return DoJoin(stream, valueJoiner, windows, props, new StreamJoinBuilder(builder, false, false), otherValueSerdes);
        }

        #endregion

        #region LeftJoin Stream

        public IKStream<K, VR> LeftJoin<V0, VR, V0S, VRS>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null) 
            where V0S : ISerDes<V0>, new()
            where VRS : ISerDes<VR>, new ()
            => LeftJoin<V0, VR, V0S, VRS>(stream, new WrappedValueJoiner<V, V0, VR>(valueJoiner), windows, props);

        public IKStream<K, VR> LeftJoin<V0, VR>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null)
            => LeftJoin(stream, new WrappedValueJoiner<V, V0, VR>(valueJoiner), windows, props);

        public IKStream<K, VR> LeftJoin<V0, VR, V0S, VRS>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null) 
            where V0S : ISerDes<V0>, new()
            where VRS : ISerDes<VR>, new ()
            => LeftJoin(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), new V0S(), new VRS());

        public IKStream<K, VR> LeftJoin<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null)
        {
            return stream is AbstractStream<K, V0> ?
                LeftJoin(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), ((AbstractStream<K, V0>)stream).ValueSerdes, null) :
                LeftJoin(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), null, null);
        }

        private IKStream<K, VR> LeftJoin<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props, ISerDes<V0> valueSerdes, ISerDes<VR> otherValueSerdes)
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

            return DoJoin(stream, valueJoiner, windows, props, new StreamJoinBuilder(builder, true, false), otherValueSerdes);
        }

        #endregion

        #region OuterJoin Stream

        public IKStream<K, VR> OuterJoin<V0, VR, V0S, VRS>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null) 
            where V0S : ISerDes<V0>, new()
            where VRS : ISerDes<VR>, new ()
            => OuterJoin<V0, VR, V0S, VRS>(stream, new WrappedValueJoiner<V, V0, VR>(valueJoiner), windows, props);


        public IKStream<K, VR> OuterJoin<V0, VR>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null)
            => OuterJoin(stream, new WrappedValueJoiner<V, V0, VR>(valueJoiner), windows, props);


        public IKStream<K, VR> OuterJoin<V0, VR, V0S, VRS>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null) 
            where V0S : ISerDes<V0>, new()
            where VRS : ISerDes<VR>, new ()
            => OuterJoin(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), new V0S(), new VRS());


        public IKStream<K, VR> OuterJoin<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null)
        {
            return stream is AbstractStream<K, V0> ?
                OuterJoin(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), ((AbstractStream<K, V0>)stream).ValueSerdes, null) :
                OuterJoin(stream, valueJoiner, windows, StreamJoinProps.From<K, V, V0>(props), null, null);
        }

        private IKStream<K, VR> OuterJoin<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props, ISerDes<V0> valueSerdes, ISerDes<VR> otherValueSerdes)
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

            return DoJoin(stream, valueJoiner, windows, props, new StreamJoinBuilder(builder, true, true), otherValueSerdes);
        }

        #endregion
        
        #region Map,FlatMap,MapValues,FlatMapValues,Foreach Async

        private IKStream<K1, V1> AsyncProcess<K1, V1>(
            string asyncProcessorName,
            string requestSinkProcessorName,
            string requestSourceProcessorName,
            string responseSinkProcessorName,
            string responseSourceProcessorName,
            RequestSerDes<K, V> requestSerDes,
            ResponseSerDes<K1, V1> responseSerDes,
            ProcessorParameters<K, V> processorParameters)
        {
            AsyncNode<K, V, K1, V1> asyncNode = new AsyncNode<K, V, K1, V1>(
                asyncProcessorName,
                requestSinkProcessorName,
                requestSourceProcessorName,
                RequestTopic(asyncProcessorName),
                responseSinkProcessorName,
                responseSourceProcessorName,
                ResponseTopic(asyncProcessorName),
                requestSerDes,
                responseSerDes,
                processorParameters);
            
            builder.AddGraphNode(Node, asyncNode.RequestNode);
            builder.AddGraphNode(asyncNode.RequestNode, asyncNode.ResponseNode);

            return new KStream<K1, V1>(requestSourceProcessorName,
                responseSerDes.ResponseKeySerDes,
                responseSerDes.ResponseValueSerDes,
                responseSourceProcessorName.ToSingle().ToList(),
                asyncNode.ResponseNode,
                builder);
        }
        
        public IKStream<K1, V1> MapAsync<K1, V1>(
            Func<ExternalRecord<K, V>, ExternalContext, Task<KeyValuePair<K1, V1>>> asyncMapper,
            RetryPolicy retryPolicy = null,
            RequestSerDes<K, V> requestSerDes = null,
            ResponseSerDes<K1, V1> responseSerDes = null,
            string named = null)
        {
            requestSerDes ??= RequestSerDes<K, V>.Empty;
            responseSerDes ??= ResponseSerDes<K1, V1>.Empty;
            
            var processors = RequestResponseProcessor(new Named(named), KStream.MAP_ASYNC_NAME);
            
            ProcessorParameters<K, V > processorParameters =
                new ProcessorParameters<K, V>(new KStreamMapAsync<K, V, K1, V1>(asyncMapper, retryPolicy), processors.asyncProcessorName);
            
            return AsyncProcess(
                processors.asyncProcessorName,
                processors.requestSinkProcessorName,
                processors.requestSourceProcessorName,
                processors.responseSinkProcessorName,
                processors.responseSourceProcessorName,
                requestSerDes,
                responseSerDes,
                processorParameters);
        }
        
        public IKStream<K, V1> MapValuesAsync<V1>(
            Func<ExternalRecord<K, V>, ExternalContext, Task<V1>> asyncMapper,
            RetryPolicy retryPolicy = null,
            RequestSerDes<K, V> requestSerDes = null,
            ResponseSerDes<K, V1> responseSerDes = null,
            string named = null)
        {
            requestSerDes ??= RequestSerDes<K, V>.Empty;
            responseSerDes ??= ResponseSerDes<K, V1>.Empty;
            
            var processors = RequestResponseProcessor(new Named(named), KStream.MAPVALUES_ASYNC_NAME);
            
            ProcessorParameters<K, V > processorParameters =
                new ProcessorParameters<K, V>(new KStreamMapValuesAsync<K, V, V1>(asyncMapper, retryPolicy), processors.asyncProcessorName);

            return AsyncProcess(
                processors.asyncProcessorName,
                processors.requestSinkProcessorName,
                processors.requestSourceProcessorName,
                processors.responseSinkProcessorName,
                processors.responseSourceProcessorName,
                requestSerDes,
                responseSerDes,
                processorParameters);
        }
        
        public IKStream<K1, V1> FlatMapAsync<K1, V1>(
            Func<ExternalRecord<K, V>, ExternalContext, Task<IEnumerable<KeyValuePair<K1, V1>>>> asyncMapper,
            RetryPolicy retryPolicy = null,
            RequestSerDes<K, V> requestSerDes = null,
            ResponseSerDes<K1, V1> responseSerDes = null,
            string named = null)
        {
            requestSerDes ??= RequestSerDes<K, V>.Empty;
            responseSerDes ??= ResponseSerDes<K1, V1>.Empty;
            
            var processors = RequestResponseProcessor(new Named(named), KStream.FLATMAP_ASYNC_NAME);

            ProcessorParameters<K, V > processorParameters =
                new ProcessorParameters<K, V>(new KStreamFlatMapAsync<K, V, K1, V1>(asyncMapper, retryPolicy), processors.asyncProcessorName);

            return AsyncProcess(
                processors.asyncProcessorName,
                processors.requestSinkProcessorName,
                processors.requestSourceProcessorName,
                processors.responseSinkProcessorName,
                processors.responseSourceProcessorName,
                requestSerDes,
                responseSerDes,
                processorParameters);
        }
        
        public IKStream<K, V1> FlatMapValuesAsync<V1>(
            Func<ExternalRecord<K, V>, ExternalContext, Task<IEnumerable<V1>>> asyncMapper,
            RetryPolicy retryPolicy = null,
            RequestSerDes<K, V> requestSerDes = null,
            ResponseSerDes<K, V1> responseSerDes = null,
            string named = null)
        {
            requestSerDes ??= RequestSerDes<K, V>.Empty;
            responseSerDes ??= ResponseSerDes<K, V1>.Empty;
            
            var processors = RequestResponseProcessor(new Named(named), KStream.FLATMAPVALUES_ASYNC_NAME);

            ProcessorParameters<K, V > processorParameters =
                new ProcessorParameters<K, V>(new KStreamFlatMapValuesAsync<K, V, V1>(asyncMapper, retryPolicy), processors.asyncProcessorName);

            return AsyncProcess(
                processors.asyncProcessorName,
                processors.requestSinkProcessorName,
                processors.requestSourceProcessorName,
                processors.responseSinkProcessorName,
                processors.responseSourceProcessorName,
                requestSerDes,
                responseSerDes,
                processorParameters);
        }
        
        public void ForeachAsync(
            Func<ExternalRecord<K, V>, ExternalContext, Task> asyncExternalCall,
            RetryPolicy retryPolicy = null,
            RequestSerDes<K, V> requestSerDes = null,
            string named = null)
        {
            requestSerDes ??= new RequestSerDes<K, V>(null, null);

            var processors = RequestResponseProcessor(new Named(named), KStream.FOREACH_ASYNC_NAME, true);

            ProcessorParameters<K, V > processorParameters =
                new ProcessorParameters<K, V>(new KStreamForeachAsync<K, V>(asyncExternalCall, retryPolicy), processors.asyncProcessorName);
            
            AsyncNode<K, V, K, V> asyncNode = new AsyncNode<K, V, K, V>(
                processors.asyncProcessorName,
                processors.requestSinkProcessorName,
                processors.requestSourceProcessorName,
                RequestTopic(processors.asyncProcessorName),
                requestSerDes,
                processorParameters);
            
            builder.AddGraphNode(Node, asyncNode.RequestNode);
        }
        
        #endregion

        #region ToTable

        public IKTable<K, V> ToTable()
            => ToTable(null);

        public IKTable<K, V> ToTable(Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null)
        {
            materialized ??= Materialized<K, V, IKeyValueStore<Bytes, byte[]>>.Create();

            var name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.TO_KTABLE_NAME);
            materialized.UseProvider(builder, KStream.TO_KTABLE_NAME);

            ISerDes<K> keySerdesOv = materialized.KeySerdes ?? KeySerdes;
            ISerDes<V> valueSerdesOv = materialized.ValueSerdes ?? ValueSerdes;

            StreamGraphNode tableParentNode = null;
            IEnumerable<string> subTopologySourceNodes = null;
            
            if (RepartitionRequired)
            {
                (string sourceName, RepartitionNode<K, V> parentNode) = CreateRepartitionSource(name, keySerdesOv, valueSerdesOv, builder);
                tableParentNode = parentNode;
                builder.AddGraphNode(Node, tableParentNode);
                subTopologySourceNodes = sourceName.ToSingle();
            }
            else
            {
                tableParentNode = Node;
                subTopologySourceNodes = this.SetSourceNodes;
            }
            
            var tableSource = new KTableSource<K, V>(materialized.StoreName, materialized.QueryableStoreName);
            var parameters = new ProcessorParameters<K, V>(tableSource, name);

            var storeBuilder = new TimestampedKeyValueStoreMaterializer<K, V>(materialized).Materialize();
            var tableNode = new StatefulProcessorNode<K, V>(name, parameters, storeBuilder);
            builder.AddGraphNode(tableParentNode, tableNode);

            return new KTable<K, V, V>(
                name,
                keySerdesOv,
                valueSerdesOv,
                subTopologySourceNodes.ToList(),
                materialized.QueryableStoreName,
                tableSource,
                tableNode,
                builder);
        }

        #endregion

        #region WithRecordTimestamp

        public IKStream<K, V> WithRecordTimestamp(Func<K, V, long> timestampExtractor, string named = null)
        {
            if (timestampExtractor == null)
            {
                throw new ArgumentNullException($"Extractor function can't be null");
            }

            String name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.RECORD_TIMESTAMP_NAME);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamTimestampExtractor<K, V>(timestampExtractor), name);
            ProcessorGraphNode<K, V> timestampExtractorNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.AddGraphNode(Node, timestampExtractorNode);

            return new KStream<K, V>(name, KeySerdes, ValueSerdes, SetSourceNodes, RepartitionRequired, timestampExtractorNode, builder);
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
            return new KStream<K, V>(name, KeySerdes, ValueSerdes, SetSourceNodes, RepartitionRequired, filterProcessorNode, builder);
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
                branchChildren[i] = new KStream<K, V>(childNames[i], KeySerdes, ValueSerdes, SetSourceNodes, RepartitionRequired, branchChildNode, builder);
            }

            return branchChildren;
        }

        private IKStream<K, V> DoMerge(IKStream<K, V> stream, string named = null)
        {
            KStream<K, V> kstream = (KStream<K, V>)stream;
            string name = new Named(named).OrElseGenerateWithPrefix(builder, KStream.MERGE_NAME);
            bool requireRepartitioning = RepartitionRequired || ((KStream<K, V>) stream).RepartitionRequired;
            ISet<string> sourceNodes = new HashSet<string>();
            sourceNodes.AddRange(SetSourceNodes);
            sourceNodes.AddRange(kstream.SetSourceNodes);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new PassThrough<K, V>(), name);
            ProcessorGraphNode<K, V> mergeNode = new ProcessorGraphNode<K, V>(name, processorParameters)
            {
                MergeNode = true
            };

            builder.AddGraphNode(new List<StreamGraphNode> { Node, kstream.Node }, mergeNode);

            // drop the serde as we cannot safely use either one to represent both streams
            return new KStream<K, V>(
                name,
                null,
                null,
                sourceNodes.ToList(),
                requireRepartitioning,
                mergeNode,
                builder);
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
                true,
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

        private KStream<K, VR> DoStreamTableJoin<V0, VR>(IKTable<K, V0> table, IValueJoiner<V, V0, VR> valueJoiner,
            Joined<K, V, V0> joined, bool leftJoin, ISerDes<VR> otherValueSerdes)
        {
            var allSourceNodes = EnsureCopartitionWith((AbstractStream<K, V0>)table);

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
                joined.KeySerdes ?? KeySerdes,
                otherValueSerdes,
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

            return new KStream<K, VR>(name, KeySerdes, null, SetSourceNodes, RepartitionRequired, joinNode, builder);
        }

        private KStream<K, V> RepartitionForJoin(
            string repartitionName,
            ISerDes<K> keySerdesOverride,
            ISerDes<V> valueSerdesOverride)
        {
            keySerdesOverride = keySerdesOverride ?? KeySerdes;
            valueSerdesOverride = valueSerdesOverride ?? ValueSerdes;

            (string repartitionSourceName, RepartitionNode<K, V> node) = CreateRepartitionSource(
                repartitionName,
                keySerdesOverride,
                valueSerdesOverride,
                builder);

            if (RepartitionNode == null || !NameNode.Equals(repartitionName))
            {
                RepartitionNode = node;
                builder.AddGraphNode(Node, RepartitionNode);
            }

            return new KStream<K, V>(
                repartitionSourceName,
                keySerdesOverride,
                valueSerdesOverride,
                repartitionSourceName.ToSingle().ToList(),
                false,
                RepartitionNode,
                builder);
        }

        private KStream<K, VR> DoJoin<V0, VR>(
            IKStream<K, V0> stream,
            IValueJoiner<V, V0, VR> valueJoiner,
            JoinWindowOptions windows,
            StreamJoinProps<K, V, V0> joinedProps,
            StreamJoinBuilder builder,
            ISerDes<VR> otherValueSerdes)
        {
            CheckIfParamNull(stream, "stream");
            CheckIfParamNull(valueJoiner, "valueJoiner");

            KStream<K, V> joinThis = this;
            KStream<K, V0> joinOther = (KStream<K, V0>)stream;
            var name = new Named(joinedProps.Name);
            
            if (joinThis.RepartitionRequired)
            {
                string leftJoinRepartitinTopicName = name.SuffixWithOrElseGet("-left", NameNode);
                joinThis = joinThis.RepartitionForJoin(leftJoinRepartitinTopicName, joinedProps.KeySerdes,
                    joinedProps.LeftValueSerdes);
            }

            if (joinOther.RepartitionRequired)
            {
                string rightJoinRepartitinTopicName = name.SuffixWithOrElseGet("-right", joinOther.NameNode);
                joinOther = joinOther.RepartitionForJoin(rightJoinRepartitinTopicName, joinedProps.KeySerdes,
                    joinedProps.RightValueSerdes);
            }

            joinThis.EnsureCopartitionWith(joinOther);
            
            return builder.Join(joinThis, joinOther, valueJoiner, windows, joinedProps, otherValueSerdes);
        }

        internal static (string, RepartitionNode<K, V>) CreateRepartitionSource(
            string repartitionTopicNameSuffix,
            ISerDes<K> keySerdes,
            ISerDes<V> valueSerdes,
            InternalStreamBuilder builder)
        {
            string repartitionTopicName = repartitionTopicNameSuffix.EndsWith(KStream.REPARTITION_TOPIC_SUFFIX)
                ? repartitionTopicNameSuffix
                : $"{repartitionTopicNameSuffix}{KStream.REPARTITION_TOPIC_SUFFIX}";

            var sinkName = builder.NewProcessorName(KStream.SINK_NAME);
            var nullKeyFilterName = builder.NewProcessorName(KStream.FILTER_NAME);
            var sourceName = builder.NewProcessorName(KStream.SOURCE_NAME);

            var processorParameters = new ProcessorParameters<K, V>(
                new KStreamFilter<K, V>((k, v) => k != null), nullKeyFilterName);

            var repartitionNode = new RepartitionNode<K, V>(
                sourceName,
                sourceName,
                processorParameters,
                keySerdes,
                valueSerdes,
                sinkName,
                repartitionTopicName);
            
            return (sourceName, repartitionNode);
        }

        private string RequestTopic(string name) => $"{name}-request";
        private string ResponseTopic(string name) => $"{name}-response";

        private (
            string requestSinkProcessorName,
            string requestSourceProcessorName,
            string responseSinkProcessorName,
            string responseSourceProcessorName,
            string asyncProcessorName
            ) RequestResponseProcessor(Named named, string asyncCst, bool onlyRequest = false)
        {
            var requestSinkProcessorName =                 
                named.SuffixWithOrElseGet("-request-sink", builder, KStream.SINK_NAME);
            var requestSourceProcessorName =
                named.SuffixWithOrElseGet("-request-source", builder, KStream.SOURCE_NAME);

            string responseSinkProcessorName = null;
            string responseSourceProcessorName = null;
            if (!onlyRequest)
            {
                responseSinkProcessorName =
                    named.SuffixWithOrElseGet("-response-sink", builder, KStream.SINK_NAME);
                responseSourceProcessorName =
                    named.SuffixWithOrElseGet("-response-source", builder, KStream.SOURCE_NAME);
            }

            var asyncProcessorName = named.OrElseGenerateWithPrefix(builder, asyncCst);

            return (
                requestSinkProcessorName,
                requestSourceProcessorName,
                responseSinkProcessorName,
                responseSourceProcessorName,
                asyncProcessorName);
        }

        #endregion
    }
}