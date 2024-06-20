using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.Stream.Internal.Graph;

namespace Streamiz.Kafka.Net.Tests.Public
{
    public class KafkaStreamTests
    {
        #region State Test

        [Test]
        public void TestCreatedState()
        {
            var state = KafkaStream.State.CREATED;
            Assert.AreEqual(0, state.Ordinal);
            Assert.AreEqual("CREATED", state.Name);
            Assert.AreEqual(new HashSet<int> { 1, 3 }, state.Transitions);
            Assert.IsFalse(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(KafkaStream.State.ERROR));
        }

        [Test]
        public void TestErrorState()
        {
            var state = KafkaStream.State.ERROR;
            Assert.AreEqual(5, state.Ordinal);
            Assert.AreEqual("ERROR", state.Name);
            Assert.AreEqual(new HashSet<int> { 3 }, state.Transitions);
            Assert.IsFalse(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(KafkaStream.State.CREATED));
            Assert.IsTrue(state.IsValidTransition(KafkaStream.State.PENDING_SHUTDOWN));
        }

        [Test]
        public void TestNotRunningState()
        {
            var state = KafkaStream.State.NOT_RUNNING;
            Assert.AreEqual(4, state.Ordinal);
            Assert.AreEqual("NOT_RUNNING", state.Name);
            Assert.AreEqual(new HashSet<int> { }, state.Transitions);
            Assert.IsFalse(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(KafkaStream.State.ERROR));
        }

        [Test]
        public void TestPendingShutdownState()
        {
            var state = KafkaStream.State.PENDING_SHUTDOWN;
            Assert.AreEqual(3, state.Ordinal);
            Assert.AreEqual("PENDING_SHUTDOWN", state.Name);
            Assert.AreEqual(new HashSet<int> { 4 }, state.Transitions);
            Assert.IsFalse(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(KafkaStream.State.CREATED));
            Assert.IsTrue(state.IsValidTransition(KafkaStream.State.NOT_RUNNING));
        }

        [Test]
        public void TestRebalancingState()
        {
            var state = KafkaStream.State.REBALANCING;
            Assert.AreEqual(1, state.Ordinal);
            Assert.AreEqual("REBALANCING", state.Name);
            Assert.AreEqual(new HashSet<int> { 2, 3, 5 }, state.Transitions);
            Assert.IsTrue(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(KafkaStream.State.CREATED));
            Assert.IsTrue(state.IsValidTransition(KafkaStream.State.RUNNING));
        }

        [Test]
        public void TestRunningState()
        {
            var state = KafkaStream.State.RUNNING;
            Assert.AreEqual(2, state.Ordinal);
            Assert.AreEqual("RUNNING", state.Name);
            Assert.AreEqual(new HashSet<int> { 1, 2, 3, 5 }, state.Transitions);
            Assert.IsTrue(state.IsRunning());
            Assert.IsFalse(state.IsValidTransition(KafkaStream.State.NOT_RUNNING));
            Assert.IsTrue(state.IsValidTransition(KafkaStream.State.PENDING_SHUTDOWN));
        }

        #endregion

        [Test]
        public async Task StartKafkaStream()
        {
            
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var t = builder.Build();
            var stream = new KafkaStream(t, config, new SyncKafkaSupplier());
            await stream.StartAsync();
            Thread.Sleep(1500);
            stream.Dispose();
        }

        [Test]
        public async Task StartKafkaStreamWithToken()
        {
            CancellationTokenSource source = new CancellationTokenSource();
            KafkaStream.State lastState = KafkaStream.State.CREATED;
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var t = builder.Build();
            var stream = new KafkaStream(t, config, new SyncKafkaSupplier());
            stream.StateChanged += (o, n) => lastState = n;
            await stream.StartAsync(source.Token);
            Thread.Sleep(1500);
            source.Cancel();
            Thread.Sleep(1500);
            Assert.AreEqual(KafkaStream.State.NOT_RUNNING, lastState);
        }

        [Test]
        public async Task StartKafkaStreamWaitRunningState()
        {
            var timeout = TimeSpan.FromSeconds(10);
            
            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";

            var builder = new StreamBuilder();
            builder.Stream<string, string>("topic").To("topic2");

            var t = builder.Build();
            var stream = new KafkaStream(t, config, new SyncKafkaSupplier());

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }

            var sensors = stream.Metrics();
            Assert.IsTrue(sensors.Any());
            
            stream.Dispose();
            Assert.IsTrue(isRunningState);
        }

        [Test]
        public async Task GetStateStore()
        {
            var timeout = TimeSpan.FromSeconds(10);
            
            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";

            var builder = new StreamBuilder();
            builder.Table("topic", InMemory.As<string,string>("store"));

            var t = builder.Build();
            var stream = new KafkaStream(t, config, new SyncKafkaSupplier());

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(isRunningState);

            if (isRunningState)
            {
                var store = stream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.KeyValueStore<string, string>())); ;
                Assert.IsNotNull(store);
            }

            stream.Dispose();
        }

        [Test]
        public async Task GetStateStoreDoesntExists()
        {
            var timeout = TimeSpan.FromSeconds(10);
            
            bool isRunningState = false;
            DateTime dt = DateTime.Now;
            var supplier = new SyncKafkaSupplier();

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";

            var builder = new StreamBuilder();
            builder.Table("topic", InMemory.As<string,string>("store"));

            var t = builder.Build();
            var stream = new KafkaStream(t, config, supplier);

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(isRunningState);

            if (isRunningState)
            {
                Assert.Throws<InvalidStateStoreException>(() => stream.Store(StoreQueryParameters.FromNameAndType("stodfdsfdsfre", QueryableStoreTypes.KeyValueStore<string, string>())));
            }

            stream.Dispose();
        }

        [Test]
        public async Task GetElementInStateStore()
        {
            var timeout = TimeSpan.FromSeconds(10);
            
            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());

            var builder = new StreamBuilder();
            builder.Table("topic", InMemory.As<string,string>("store"));

            var t = builder.Build();
            var stream = new KafkaStream(t, config, supplier);

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(isRunningState);

            if (isRunningState)
            {
                var serdes = new StringSerDes();
                producer.Produce("topic",
                    new Confluent.Kafka.Message<byte[], byte[]>
                    {
                        Key = serdes.Serialize("key1", new SerializationContext()),
                        Value = serdes.Serialize("coucou", new SerializationContext())
                    });
                Thread.Sleep(50);
                var store = stream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.KeyValueStore<string, string>()));
                Assert.IsNotNull(store);
                Assert.AreEqual(1, store.ApproximateNumEntries());
                var item = store.Get("key1");
                Assert.IsNotNull(item);
                Assert.AreEqual("coucou", item);
            }

            stream.Dispose();
        }

        [Test]
        public async Task GetRangeKVStateStore()
        {
            var timeout = TimeSpan.FromSeconds(10);

            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());

            var builder = new StreamBuilder();
            builder.Table("topic", InMemory.As<string,string>("store"));

            var t = builder.Build();
            var stream = new KafkaStream(t, config, supplier);

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(isRunningState);

            if (isRunningState)
            {
                var serdes = new StringSerDes();
                producer.Produce("topic",
                    new Confluent.Kafka.Message<byte[], byte[]>
                    {
                        Key = serdes.Serialize("key1", new SerializationContext()),
                        Value = serdes.Serialize("coucou", new SerializationContext())
                    });
                Thread.Sleep(50);
                var store = stream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.KeyValueStore<string, string>()));
                Assert.IsNotNull(store);
                var list = store.Range("key1", "key2").ToList();
                Assert.AreEqual(1, list.Count);
                var item = list[0];
                Assert.IsNotNull(item);
                Assert.AreEqual("coucou", item.Value);
                Assert.AreEqual("key1", item.Key);
            }

            stream.Dispose();
        }
        
        [Test]
        public void GetStateStoreBeforeRunningState()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";

            var builder = new StreamBuilder();
            builder.Table("topic", InMemory.As<string,string>("store"));

            var t = builder.Build();
            var stream = new KafkaStream(t, config, new SyncKafkaSupplier());
            Assert.Throws<IllegalStateException>(() => stream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.KeyValueStore<string, string>())));
            stream.Dispose();
        }

        [Test]
        public async Task GetWindowStateStore()
        {
            var timeout = TimeSpan.FromSeconds(10);
            
            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(1)))
                .Count(InMemoryWindows.As<string,long>("store"));

            var t = builder.Build();
            var stream = new KafkaStream(t, config, new SyncKafkaSupplier());

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(isRunningState);

            if (isRunningState)
            {
                var store = stream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.WindowStore<string, long>()));
                Assert.IsNotNull(store);
            }

            stream.Dispose();
        }

        [Test]
        public async Task GetWindowElementInStateStore()
        {
            var timeout = TimeSpan.FromSeconds(10);
            
            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(1)))
                .Count(InMemoryWindows.As<string,long>("store"));

            var t = builder.Build();
            var stream = new KafkaStream(t, config, supplier);

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(isRunningState);

            if (isRunningState)
            {
                var serdes = new StringSerDes();
                dt = DateTime.Now;
                producer.Produce("test",
                    new Confluent.Kafka.Message<byte[], byte[]>
                    {
                        Key = serdes.Serialize("key1", new SerializationContext()),
                        Value = serdes.Serialize("coucou", new SerializationContext()),
                        Timestamp = new Confluent.Kafka.Timestamp(dt)
                    });
                Thread.Sleep(50);
                var store = stream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.WindowStore<string, long>()));
                Assert.IsNotNull(store);
                var @enum = store.All();
                Assert.AreEqual(1, store.All().ToList().Count);
                var item = store.Fetch("key1", dt.AddMinutes(-1), dt.AddMinutes(1));
                Assert.IsNotNull(item);
                Assert.IsTrue(item.MoveNext());
                Assert.IsTrue(item.Current.HasValue);
                Assert.AreEqual(1, item.Current.Value.Value);
                item.Dispose();
            }

            stream.Dispose();
        }

        [Test]
        public async Task GetWStateStoreInvalidStateStoreException()
        {
            var timeout = TimeSpan.FromSeconds(10);
            
            bool state = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .GroupByKey()
                .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(1)))
                .Count(InMemoryWindows.As<string,long>("store"));

            var t = builder.Build();
            var stream = new KafkaStream(t, config, new SyncKafkaSupplier());

            stream.StateChanged += (old, @new) =>
            {
                if (!@new.Equals(KafkaStream.State.RUNNING))
                {
                    if (!state)
                    {
                        Assert.Throws<InvalidStateStoreException>(() => stream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.WindowStore<string, long>())));
                        state = true;
                    }
                }
            };
            await stream.StartAsync();
            while (!state)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(state);

            stream.Dispose();
        }

        [Test]
        public async Task GetKVStateStoreInvalidStateStoreException()
        {
            var timeout = TimeSpan.FromSeconds(10);
            
            bool state = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("test")
                .GroupByKey()
                .Count(InMemory.As<string,long>("store"));

            var t = builder.Build();
            var stream = new KafkaStream(t, config, new SyncKafkaSupplier());

            stream.StateChanged += (old, @new) =>
            {
                if (!@new.Equals(KafkaStream.State.RUNNING))
                {
                    if (!state)
                    {
                        Assert.Throws<InvalidStateStoreException>(() => stream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.KeyValueStore<string, long>())));
                        state = true;
                    }
                }
            };
            await stream.StartAsync();
            while (!state)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(state);

            stream.Dispose();
        }

        [Test]
        public async Task BuildGlobalStateStore()
        {
            var timeout = TimeSpan.FromSeconds(10);
            
            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 1;

            var builder = new StreamBuilder();
            builder.GlobalTable<string, string>("test", InMemory.As<string,string>("store"));

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(new ProducerConfig());
            var t = builder.Build();
            var stream = new KafkaStream(t, config, supplier);

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(isRunningState);

            if (isRunningState)
            {
                var stringSerdes = new StringSerDes();
                producer.Produce("test",
                    new Message<byte[], byte[]>
                    {
                        Key = stringSerdes.Serialize("key", new SerializationContext()),
                        Value = stringSerdes.Serialize("value", new SerializationContext())
                    });

                Thread.Sleep(250);
                var store = stream.Store(StoreQueryParameters.FromNameAndType("store", QueryableStoreTypes.KeyValueStore<string, string>()));
                Assert.IsNotNull(store);
                Assert.AreEqual(1, store.ApproximateNumEntries());
            }

            stream.Dispose();
        }
        
        [Test]
        public async Task ExternalCall()
        {
            var timeout = TimeSpan.FromSeconds(10);
            
            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;

            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .MapAsync(
                    async (record, _) =>
                    {
                        await Task.Delay(100);
                        return await Task.FromResult(
                            new KeyValuePair<string, string>(record.Key, record.Value.ToUpper()));
                    })
                .To("output");

            var supplier = new SyncKafkaSupplier();
            
            supplier
                .GetAdmin(new AdminClientConfig())
                .CreateTopicsAsync(new List<TopicSpecification>()
                {
                    new(){Name = "input", NumPartitions = 1},
                    new(){Name = "output", NumPartitions = 1}
                })
                .GetAwaiter()
                .GetResult(); ;
            
            var producer = supplier.GetProducer(new ProducerConfig());
            var t = builder.Build();
            
            var stream = new KafkaStream(t, config, supplier);

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }
            Assert.IsTrue(isRunningState);

            if (isRunningState)
            {
                var stringSerdes = new StringSerDes();
                producer.Produce("input",
                    new Message<byte[], byte[]>
                    {
                        Key = stringSerdes.Serialize("key", new SerializationContext()),
                        Value = stringSerdes.Serialize("value", new SerializationContext())
                    });

                var consumerConfig = config.Clone().ToConsumerConfig("outside");
                consumerConfig.GroupId = "random-outside";
                var outsideConsumer = supplier.GetConsumer(consumerConfig, null);

                ConsumeResult<byte[], byte[]> r = null;
                outsideConsumer.Subscribe("output");
                DateTime now = DateTime.Now;
                do
                {
                    r = outsideConsumer.Consume();
                } while (r == null && now.Add(timeout) > DateTime.Now);
                    
                Assert.IsNotNull(r);
                Assert.AreEqual(stringSerdes.Deserialize(r.Message.Key, SerializationContext.Empty), "key");
                Assert.AreEqual(stringSerdes.Deserialize(r.Message.Value, SerializationContext.Empty), "VALUE");
            }

            stream.Dispose();
        }

        private sealed class OpenIteratorProcessorTest : IProcessor<string, string>
        {
            private readonly string _storeName;
            private readonly List<IKeyValueEnumerator<string, string>> _currentEnumerators;
            private IKeyValueStore<string, string> store;

            public OpenIteratorProcessorTest()
            {
            }
            
            public OpenIteratorProcessorTest(string storeName, List<IKeyValueEnumerator<string, string>> currentEnumerators)
            {
                _storeName = storeName;
                _currentEnumerators = currentEnumerators;
            }
            public void Init(ProcessorContext<string, string> context)
            {
                store = (IKeyValueStore<string, string>)context.GetStateStore(_storeName);
            }

            public void Process(Record<string, string> record)
            {
                var enumerator = store.Range("key0", "key9");
                enumerator.MoveNext();
                store.Put(record.Key, record.Value);
                // enumerator is not closed
                _currentEnumerators.Add(enumerator);
            }

            public void Close()
            {
                
            }
        }
        
        [Test]
        public async Task OpenIteratorsWithTheProcessAPI()
        {
            var timeout = TimeSpan.FromSeconds(10);

            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-open-iterator";
            config.BootstrapServers = "127.0.0.1";
            config.PollMs = 10;

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());

            var currentEnumerators = new List<IKeyValueEnumerator<string, string>>();
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input")
                .Process(new ProcessorBuilder<string, string>()
                    .Processor<OpenIteratorProcessorTest>("store", currentEnumerators)
                    .StateStore(State.Stores.KeyValueStoreBuilder(
                            State.Stores.PersistentKeyValueStore("store"),
                            new StringSerDes(),
                            new StringSerDes()))
                    .Build());
            
            var t = builder.Build();
            var stream = new KafkaStream(t, config, supplier);

            stream.StateChanged += (old, @new) =>
            {
                if (@new.Equals(KafkaStream.State.RUNNING))
                {
                    isRunningState = true;
                }
            };
            await stream.StartAsync();
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                {
                    break;
                }
            }

            Assert.IsTrue(isRunningState);

            if (isRunningState)
            {
                var serdes = new StringSerDes();
                for (int i = 0; i < 10; ++i)
                {
                    producer.Produce("input",
                        new Confluent.Kafka.Message<byte[], byte[]> {
                            Key = serdes.Serialize($"key{i}", new SerializationContext()),
                            Value = serdes.Serialize($"coucou{i}", new SerializationContext())
                        });
                }

                Thread.Sleep(50);
                var store = stream.Store(StoreQueryParameters.FromNameAndType("store",
                    QueryableStoreTypes.KeyValueStore<string, string>()));
                Assert.IsNotNull(store);
                while (store.All().ToList().Count != 10) ;
            }
            
            Assert.AreEqual(10, currentEnumerators.Count);
            stream.Dispose();
            foreach(var e in currentEnumerators)
                Assert.Throws<ObjectDisposedException>(() => e.Dispose());
        }
    }
}