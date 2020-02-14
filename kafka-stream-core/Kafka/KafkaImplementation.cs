using Confluent.Kafka;
using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_stream_core.Kafka
{
    internal class KafkaImplementation : IKafkaClient
    {
        protected readonly IDictionary<string, IProducer<byte[], byte[]>> _producers = new Dictionary<string, IProducer<byte[], byte[]>>();
        protected readonly IDictionary<string, IConsumer<byte[], byte[]>> _consumers = new Dictionary<string, IConsumer<byte[], byte[]>>();
        protected readonly IDictionary<string, Task> _consumerReceiver = new Dictionary<string, Task>();

        protected readonly Configuration configuration;
        protected readonly IKafkaSupplier kafkaSupplier;
        protected CancellationTokenSource cts = new CancellationTokenSource();

        internal KafkaImplementation(Configuration configuration, IKafkaSupplier kafkaSupplier)
        {
            this.configuration = configuration;
            this.kafkaSupplier = kafkaSupplier;
        }

        private string GetKey<K, V>() => $"{typeof(K).Name}-{typeof(V).Name}";

        private IProducer<byte[], byte[]> CreateGenericProducer(string clef)
        {
            ProducerConfig producer = configuration.toProducerConfig();
            var p = this.kafkaSupplier.GetProducer(producer);
            _producers.Add(clef, p);
            return p;
        }

        private IConsumer<byte[], byte[]> CreateGenericConsumer(string clef)
        {
            ConsumerConfig consumer = configuration.toConsumerConfig();
            var c = this.kafkaSupplier.GetConsumer(consumer);
            _consumers.Add(clef, c);
            return c;
        }

        public void Publish<K, V>(K key, V value, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, string topicName)
        {
            var clef = GetKey<K, V>();
            IProducer<byte[], byte[]> producer = null;
            if (!_producers.ContainsKey(clef))
                producer = CreateGenericProducer(clef);
            else
            {
                producer = _producers[clef];
            }

            var k = keySerdes.Serialize(key);
            var v = valueSerdes.Serialize(value);
            producer.Produce(topicName, new Message<byte[], byte[]> { Key = k, Value = v });
            producer.Flush(cts.Token);
        }

        public void Subscribe<K, V>(string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, Action<K, V> action)
        {
            var clef = GetKey<K, V>();
            IConsumer<byte[], byte[]> consumer = null;
            if (!_consumers.ContainsKey(clef))
            {
                consumer = CreateGenericConsumer(clef);
                consumer.Subscribe(topicName);
            }
            else
            {
                consumer = _consumers[clef];
            }

            Task t = Task.Run(() =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(cts.Token);
                        if (cr != null)
                        {
                            var key = keySerdes.Deserialize(cr.Key);
                            var value = valueSerdes.Deserialize(cr.Value);
                            action?.Invoke(key, value);
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                        break;
                    }
                    catch(OperationCanceledException e)
                    {
                        break;
                    }
                }
            });

            _consumerReceiver.Add(clef, t);
        }

        public void Unsubscribe<K, V>(string topicName)
        {
            var clef = GetKey<K, V>();
            (_consumers[clef] as IConsumer<K, V>).Unassign();
            (_consumers[clef] as IConsumer<K, V>).Unsubscribe();
        }

        public void Dispose()
        {
            cts.Cancel();

            foreach (var k in _consumerReceiver)
                k.Value.Wait();

            foreach (var k in _producers)
                if (k.Value is IDisposable)
                    ((IDisposable)(k.Value)).Dispose();

            foreach (var k in _consumers)
                if (k.Value is IDisposable)
                    ((IDisposable)(k.Value)).Dispose();
        }
    }
}
