using Confluent.Kafka;
using kafka_stream_core.Crosscutting;
using kafka_stream_core.SerDes;
using System;
using System.Threading;

namespace kafka_stream_core.Processors
{
    public class StreamThread : IThread
    {
        #region IThread Impl

        public string Name { get; }

        public bool IsRunning { get; private set; } = false;

        public bool IsDisposable { get; private set; } = false;

        public int Id => thread.ManagedThreadId;

        public void Dispose()
        {
            IsRunning = false;
            consumer.Unsubscribe();
            processor.Close();
            thread.Join();
            IsDisposable = true;
        }

        public void Run()
        {
            while (IsRunning)
            {
                var records = consumer.ConsumeRecords(time);
                foreach (var r in records)
                {
                    object key = null, value = null;

                    if(r.Key != null)
                        key = processor.Key.DeserializeObject(r.Key);
                    if(r.Value != null)
                        value = processor.Key.DeserializeObject(r.Value);

                    processor.Process(key, value);
                }
            }
        }

        public void Start()
        {
            IsRunning = true;
            processor.Init(context);
            consumer.Subscribe(((ISourceProcessor)processor).TopicName);
            thread.Start();
        }

        #endregion

        private IProcessor processor;
        private ProcessorContext context;
        private Thread thread;
        private IConsumer<byte[], byte[]> consumer;
        private TimeSpan time;

        internal static IThread create(
            string name,
            IConsumer<byte[], byte[]> consumer,
            ProcessorContext context, 
            IProcessor processor, 
            int consumeTimeoutms = 1000)
        {
            return new StreamThread(name, consumer, context, processor, consumeTimeoutms);
        }

        private StreamThread(string name, IConsumer<byte[], byte[]> consumer, ProcessorContext context, IProcessor processor, int consumeTimeoutms)
        {
            Name = name;
            this.context = context;
            this.processor = processor;
            this.consumer = consumer;
            this.time = TimeSpan.FromMilliseconds(consumeTimeoutms);

            this.thread = new Thread(this.Run);
        }
    }
}
