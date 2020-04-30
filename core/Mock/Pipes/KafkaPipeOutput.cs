using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock.Pipes
{
    internal class KafkaPipeOutput : IPipeOutput
    {
        private readonly string topicName;
        private readonly TimeSpan timeout;
        private readonly ILog logger = Logger.GetLogger(typeof(KafkaPipeOutput));

        private readonly CancellationToken token;
        private readonly Thread readThread;
        private readonly IConsumer<byte[], byte[]> consumer;
        private readonly Queue<(byte[], byte[])> queue = new Queue<(byte[], byte[])>();
        private readonly object _lock = new object();

        public int Size
        {
            get
            {
                lock (_lock)
                    return queue.Count;
            }
        }

        public bool IsEmpty
        {
            get
            {
                var infos = this.GetInfos();
                foreach (var i in infos)
                    if (i.Offset < i.High)
                        return false;
                return true;
            }
        }

        public KafkaPipeOutput(string topicName, TimeSpan timeout, IStreamConfig configuration, IKafkaSupplier kafkaSupplier, CancellationToken token)
        {
            this.topicName = topicName;
            this.timeout = timeout;

            this.consumer = kafkaSupplier.GetConsumer(configuration.ToConsumerConfig($"pipe-output-{configuration.ApplicationId}-{topicName}"), null);
            
            this.token = token;
            this.readThread = new Thread(ReadThread);
            this.readThread.Start();
        }

        private void ReadThread()
        {
            this.consumer.Subscribe(this.topicName);

            while (!token.IsCancellationRequested)
            {
                var record = consumer.Consume(timeout);
                if (record != null)
                {
                    lock (_lock)
                        queue.Enqueue((record.Message.Key, record.Message.Value));
                    consumer.Commit(record);
                }
            }
        }

        public void Dispose()
        {
            if (queue.Count > 0)
                logger.Warn($"Dispose pipe queue for topic {this.topicName} whereas it's not empty (Size : {queue.Count})");

            consumer.Unsubscribe();
            readThread.Join();
            consumer.Dispose();
        }

        public KeyValuePair<byte[], byte[]> Read()
        {
            int count = 0;
            while (count <= 10)
            {
                int size = 0;
                lock (_lock)
                    size = queue.Count;

                if (size > 0)
                {
                    var record = queue.Dequeue();
                    return new KeyValuePair<byte[], byte[]>(record.Item1, record.Item2);
                }
                else
                {
                    Thread.Sleep((int)timeout.TotalMilliseconds / 10);
                    ++count;
                }
            }

            throw new StreamsException($"No record found in topic {topicName} after {timeout.TotalSeconds}s !");
        }

        public List<PipeOutputInfo> GetInfos()
        {
            List<PipeOutputInfo> l = new List<PipeOutputInfo>();
            var watermark = this.consumer.GetWatermarkOffsets();
            var offsets = this.consumer.Committed(TimeSpan.FromSeconds(1));
            foreach(var o in offsets)
            {
                var w = watermark.FirstOrDefault(f => f.Topic.Equals(o.Topic) && f.Partition.Equals(o.Partition));
                l.Add(new PipeOutputInfo
                {
                    Offset = o.Offset.Value,
                    Topic = o.Topic,
                    Partition = o.Partition,
                    High = w != null ? w.High.Value : o.Offset.Value,
                    Low = w != null ? w.Low.Value : o.Offset.Value
                });
            }
            return l;
        }

        public IEnumerable<KeyValuePair<byte[], byte[]>> ReadList()
        {
            List<KeyValuePair<byte[], byte[]>> records = new List<KeyValuePair<byte[], byte[]>>();
            int count = 0;
            while (count <= 10)
            {
                int size = 0;
                lock (_lock)
                    size = queue.Count;

                if (size > 0)
                {
                    for (int i = 0; i < size; ++i)
                    {
                        var r = queue.Dequeue();
                        records.Add(new KeyValuePair<byte[], byte[]>(r.Item1, r.Item2));
                    }
                    return records;
                }
                else
                {
                    Thread.Sleep((int)timeout.TotalMilliseconds / 10);
                    ++count;
                }
            }
            return records;
        }
    }
}
