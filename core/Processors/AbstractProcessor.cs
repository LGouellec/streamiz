using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;


namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractProcessor<K, V> : IProcessor<K, V>
    {
        protected ILogger log = null;
        protected string logPrefix = "";
        protected Sensor droppedRecordsSensor;


        public ProcessorContext Context { get; protected set; }

        public string Name { get; set; }
        public IList<string> StateStores { get; protected set; }

        public ISerDes<K> KeySerDes => Key is ISerDes<K> ? (ISerDes<K>)Key : null;

        public ISerDes<V> ValueSerDes => Value is ISerDes<V> ? (ISerDes<V>)Value : null;

        public ISerDes Key { get; internal set; } = null;

        public ISerDes Value { get; internal set; } = null;

        public IList<IProcessor> Next { get; private set; } = new List<IProcessor>();

        #region Ctor

        protected AbstractProcessor()
            : this(null)
        {

        }

        protected AbstractProcessor(string name)
            : this(name, null, null)
        {
        }

        protected AbstractProcessor(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : this(name, keySerdes, valueSerdes, null)
        {
        }

        protected AbstractProcessor(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> stateStores)
        {
            Name = name;
            Key = keySerdes;
            Value = valueSerdes;
            StateStores = stateStores != null ? new List<string>(stateStores) : new List<string>();
            log = Logger.GetLogger(GetType());
        }

        #endregion

        public virtual void Close()
        {
            foreach (var n in Next)
            {
                n.Close();
            }
            
            Context.Metrics.RemoveNodeSensors(Thread.CurrentThread.Name, Context.Id.ToString(), Name);
        }

        #region Forward

        public virtual void Forward<K1, V1>(K1 key, V1 value, long ts)
        {
            Context.ChangeTimestamp(ts);
            Forward(key, value);
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value)
        {
            log.LogDebug(
                "{LogPrefix}Forward<{KeyType},{ValueType}> message with key {Key} and value {Value} to each next processor",
                logPrefix, typeof(K1).Name, typeof(V1).Name, key, value);

            Forward(Next.OfType<IProcessor<K1, V1>>(), processor => processor.Process(key, value));
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value, string name)
        {
            var processors = Next.OfType<IProcessor<K1, V1>>().Where(processor => processor.Name.Equals(name));
            Forward(processors, processor =>
            {
                log.LogDebug(
                    "{LogPrefix}Forward<{KeyType},{ValueType}> message with key {Key} and value {Value} to processor {Processor}",
                    logPrefix,
                    typeof(K1).Name, typeof(V1).Name, key, value, name);
                processor.Process(key, value);
            });
        }

        public virtual void Forward(K key, V value)
        {
            log.LogDebug(
                "{LogPrefix}Forward<{KeyType},{ValueType}> message with key {Key} and value {Value} to each next processor",
                logPrefix, typeof(K).Name, typeof(V).Name, key, value);
            
            Forward(Next, genericProcessor =>
            {
                if (genericProcessor is IProcessor<K, V> processor)
                    processor.Process(key, value);
                else
                    genericProcessor.Process(key, value);
            });
        }

        public virtual void Forward(K key, V value, string name)
        {
            Forward(Next.Where(processor => processor.Name.Equals(name)), genericProcessor =>
            {
                log.LogDebug(
                    "{LogPrefix}Forward<{KeyType},{ValueType}> message with key {Key} and value {Value} to processor {Processor}",
                    logPrefix,
                    typeof(K).Name, typeof(V).Name, key, value, name);
                if (genericProcessor is IProcessor<K, V> processor)
                    processor.Process(key, value);
                else
                    genericProcessor.Process(key, value);
            });
        }

        private void Forward(IEnumerable<IProcessor> processors, Action<IProcessor> action)
        {
            if (Context.Configuration.ParallelProcessing)
            {
                try
                {
                    // feature disabled when the processing is asynchronous
                    Context.CurrentProcessor = null;
                    Parallel.ForEach(processors,
                        new ParallelOptions { MaxDegreeOfParallelism = Context.Configuration.MaxDegreeOfParallelism },
                        action);
                }
                catch (AggregateException e)
                {
                    throw e.GetBaseException();
                }
            }
            else
            {
                foreach (var processor in processors)
                {
                    Context.CurrentProcessor = processor;
                    action.Invoke(processor);
                }
            }
        }

        #endregion

        public virtual void Init(ProcessorContext context)
        {
            log.LogDebug("{LogPrefix}Initializing process context", logPrefix);
            Context = context;
            Context.CurrentProcessor = this;
            droppedRecordsSensor = TaskMetrics.DroppedRecordsSensor(
                Thread.CurrentThread.Name,
                Context.Id,
                Context.Metrics);
            
            foreach (var n in Next)
            {
                Context.CurrentProcessor = n;
                n.Init(context);
            }
            
            Context.CurrentProcessor = this;
            log.LogDebug("{LogPrefix}Process context initialized", logPrefix);
        }

        protected void LogProcessingKeyValue(K key, V value) => log.LogDebug(
            $"{logPrefix}Process<{typeof(K).Name},{typeof(V).Name}> message with key {key} and {value}" +
            $" with record metadata [topic:{Context.RecordContext.Topic}|" +
            $"partition:{Context.RecordContext.Partition}|offset:{Context.RecordContext.Offset}]");

        #region Setter

        public void SetTaskId(TaskId id)
        {
            logPrefix = $"stream-task[{id.Id}|{id.Partition}]|processor[{Name}]- ";
            foreach(var n in Next)
                n.SetTaskId(id);
        }

        public void AddNextProcessor(IProcessor next)
        {
            if (next != null && !Next.Contains(next as IProcessor))
                Next.Add(next);
        }

        #endregion

        #region Process object

        public void Process(ConsumeResult<byte[], byte[]> record)
        {
            bool throwException = false;
            ObjectDeserialized key = null;
            ObjectDeserialized value = null;

            if (KeySerDes != null)
            {
                key = DeserializeKey(record);
                if (key.MustBeSkipped)
                {
                    log.LogDebug(
                        "{LogPrefix} Message with record metadata [topic:{Topic}|partition:{Partition}|offset:{Offset}] was skipped !",
                        logPrefix, Context.RecordContext.Topic, Context.RecordContext.Partition,
                        Context.RecordContext.Offset);
                    droppedRecordsSensor.Record();
                    return;
                }
            }
            else
                throwException = true;

            if (ValueSerDes != null)
            {
                value = DeserializeValue(record);
                if (value.MustBeSkipped)
                {
                    log.LogDebug(
                        "{LogPrefix} Message with record metadata [topic:{Topic}|partition:{Partition}|offset:{Offset}] was skipped !",
                        logPrefix, Context.RecordContext.Topic, Context.RecordContext.Partition,
                        Context.RecordContext.Offset);
                    droppedRecordsSensor.Record();
                    return;
                }
            }
            else
                throwException = true;

            if (throwException)
            {
                var s = KeySerDes == null ? "key" : "value";
                log.LogError(
                    "{LogPrefix}Impossible to receive source data because keySerdes and/or valueSerdes is not setted ! KeySerdes : {KeySerdes} | ValueSerdes : {ValueSerdes}",
                    logPrefix, KeySerDes != null ? KeySerDes.GetType().Name : "NULL",
                    ValueSerDes != null ? ValueSerDes.GetType().Name : "NULL");
                throw new StreamsException($"{logPrefix}The {s} serdes is not compatible to the actual {s} for this processor. Change the default {s} serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
            }
            else
                Process(key.Bean, value.Bean);
        }

        public void Process(object key, object value)
        {
            if ((key == null || key is K) && (value == null || value is V))
            {
                Context.CurrentProcessor = this;
                Process((K) key, (V) value);
            }
        }

        #endregion

        #region Abstract

        public abstract void Process(K key, V value);

        #endregion

        public override bool Equals(object obj)
        {
            return obj is AbstractProcessor<K, V> && ((AbstractProcessor<K, V>)obj).Name.Equals(Name);
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }

        public virtual ObjectDeserialized DeserializeKey(ConsumeResult<byte[], byte[]> record)
        {
            try
            {
                var o = Key.DeserializeObject(record.Message.Key, new SerializationContext(MessageComponentType.Key, record.Topic, record.Message.Headers));
                return new ObjectDeserialized(o, false);
            }catch(Exception e)
            {
                var handlerResponse = Context.Configuration.DeserializationExceptionHandler != null ?
                    Context.Configuration.DeserializationExceptionHandler(Context, record, e) : ExceptionHandlerResponse.FAIL;

                if (handlerResponse == ExceptionHandlerResponse.FAIL)
                    throw new DeserializationException($"{ logPrefix }Error during key deserialization[Topic:{ record.Topic}| Partition:{ record.Partition}| Offset:{ record.Offset}| Timestamp:{ record.Message.Timestamp.UnixTimestampMs}]", e);
                else
                {
                    StringBuilder sb = new StringBuilder();
                    sb.AppendLine($"{logPrefix}Error during key deserialization [Topic:{record.Topic}|Partition:{record.Partition}|Offset:{record.Offset}|Timestamp:{record.Message.Timestamp.UnixTimestampMs}] with exception {e}.");
                    sb.AppendLine($"{logPrefix}DeserializationExceptionHandler return 'CONTINUE', so this message will be skipped and not processed !");
                    log.LogError(sb.ToString());
                    return ObjectDeserialized.ObjectSkipped;
                }
            }

        }

        public virtual ObjectDeserialized DeserializeValue(ConsumeResult<byte[], byte[]> record)
        {
            try
            {
                var o = Value.DeserializeObject(record.Message.Value, new SerializationContext(MessageComponentType.Value, record.Topic, record.Message.Headers));
                return new ObjectDeserialized(o, false);
            }
            catch (Exception e)
            {
                var handlerResponse = Context.Configuration.DeserializationExceptionHandler != null ?
                    Context.Configuration.DeserializationExceptionHandler(Context, record, e) : ExceptionHandlerResponse.FAIL;

                if (handlerResponse == ExceptionHandlerResponse.FAIL)
                    throw new DeserializationException($"{ logPrefix }Error during value deserialization[Topic:{ record.Topic}| Partition:{ record.Partition}| Offset:{ record.Offset}| Timestamp:{ record.Message.Timestamp.UnixTimestampMs}]", e);
                else
                {
                    StringBuilder sb = new StringBuilder();
                    sb.AppendLine($"{logPrefix}Error during value deserialization [Topic:{record.Topic}|Partition:{record.Partition}|Offset:{record.Offset}|Timestamp:{record.Message.Timestamp.UnixTimestampMs}] with exception {e}.");
                    sb.AppendLine($"{logPrefix}DeserializationExceptionHandler return 'CONTINUE', so this message will be skipped and not processed !");
                    log.LogError(sb.ToString());
                    return ObjectDeserialized.ObjectSkipped;
                }
            }
        }
    }
}
