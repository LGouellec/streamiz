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
using System.Threading;
using System.Threading.Tasks;

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
            builder.Table("topic", InMemory<string, string>.As("store"));

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
            builder.Table("topic", InMemory<string, string>.As("store"));

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
            builder.Table("topic", InMemory<string, string>.As("store"));

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
        public void GetStateStoreBeforeRunningState()
        {
            

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test";
            config.BootstrapServers = "127.0.0.1";

            var builder = new StreamBuilder();
            builder.Table("topic", InMemory<string, string>.As("store"));

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
                .Count(InMemoryWindows<string, long>.As("store"));

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
                .Count(InMemoryWindows<string, long>.As("store"));

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
                .Count(InMemoryWindows<string, long>.As("store"));

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
                .Count(InMemory<string, long>.As("store"));

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
            builder.GlobalTable<string, string>("test", InMemory<string, string>.As("store"));

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

    }
}