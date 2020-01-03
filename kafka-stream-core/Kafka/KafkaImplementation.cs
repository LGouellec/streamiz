using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_stream_core.Kafka
{
    internal class KafkaImplementation : IKafkaClient
    {
        protected readonly IDictionary<string, object> _producers = new Dictionary<string, object>();
        protected readonly IDictionary<string, object> _consumers = new Dictionary<string, object>();
        protected readonly IDictionary<string, Task> _consumerReceiver = new Dictionary<string, Task>();

        protected readonly Configuration configuration;
        protected CancellationTokenSource cts = new CancellationTokenSource();

        internal KafkaImplementation(Configuration configuration)
        {
            this.configuration = configuration;
        }

        private string GetKey<K, V>() => $"{typeof(K).Name}-{typeof(V).Name}";

        private IProducer<K, V> CreateGenericProducer<K, V>(string clef)
        {
            // TODO : Avro Serializer
            ProducerConfig producer = configuration.toProducerConfig();

            var builder = new ProducerBuilder<K, V>(producer) { };
            var p = builder.Build();
            _producers.Add(clef, p);
            return p;
        }

        private IConsumer<K, V> CreateGenericConsumer<K, V>(string clef)
        {
            // TODO : Avro Serializer
            ConsumerConfig consumer = configuration.toConsumerConfig();

            var builder = new ConsumerBuilder<K, V>(consumer) { };
            var c = builder.Build();
            _consumers.Add(clef, c);
            return c;
        }

        public void Publish<K, V>(K key, V value, string topicName)
        {
            var clef = GetKey<K, V>();
            IProducer<K, V> producer = null;
            if (!_producers.ContainsKey(clef))
                producer = CreateGenericProducer<K, V>(clef);
            else
            {
                if (_producers[clef] is IProducer<K, V>)
                    producer = _producers[clef] as IProducer<K, V>;
            }

            producer.Produce(topicName, new Message<K, V> { Key = key, Value = value });
            producer.Flush(cts.Token);
        }

        public void Subscribe<K, V>(string topicName, Action<K, V> action)
        {
            var clef = GetKey<K, V>();
            IConsumer<K, V> consumer = null;
            if (!_consumers.ContainsKey(clef))
            {
                consumer = CreateGenericConsumer<K, V>(clef);
                consumer.Subscribe(topicName);
            }
            else
            {
                if (_consumers[clef] is IConsumer<K, V>)
                    consumer = _consumers[clef] as IConsumer<K, V>;
            }

            Task t = Task.Run(() =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(cts.Token);
                        if (cr != null)
                            action?.Invoke(cr.Key, cr.Value);
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
