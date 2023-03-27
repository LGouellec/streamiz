using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Private
{
    public class TaskScheduledTests
    {
        class MySystemProcessor : IProcessor<string, string>
        {
            public long count { get; set; }
            
            public void Init(ProcessorContext<string, string> context)
            {
                context.Schedule(
                    TimeSpan.FromMilliseconds(10),
                    PunctuationType.WALL_CLOCK_TIME,
                    (now) => {
                        ++count;
                    });
            }

            public void Process(Record<string, string> record)
            {
                
            }

            public void Close()
            {
                
            }
        }
        
        class MyEventProcessor : IProcessor<string, string>
        {
            public long count { get; set; }
            
            public void Init(ProcessorContext<string, string> context)
            {
                context.Schedule(
                    TimeSpan.FromMilliseconds(10),
                    PunctuationType.STREAM_TIME,
                    (now) => {
                        ++count;
                    });
            }

            public void Process(Record<string, string> record)
            {
                
            }

            public void Close()
            {
                
            }
        }

        class MyCloseProcessor : IProcessor<string, string>
        {
            private TaskScheduled taskScheduled;
            public long count { get; set; }

            public void Init(ProcessorContext<string, string> context)
            {
                taskScheduled = context.Schedule(
                    TimeSpan.FromMilliseconds(10),
                    PunctuationType.STREAM_TIME,
                    (now) => {
                        ++count;
                    });
            }

            public void Process(Record<string, string> record)
            {
                if(count > 0 && !taskScheduled.IsCancelled)
                    taskScheduled.Cancel();
            }

            public void Close()
            {
                
            }
        }

        class ProcessorThrowableException : IProcessor<string, string>
        {
            private readonly bool throwTaskMigration;
            private readonly bool throwStreamsException;
            private readonly bool throwKafkaException;

            public ProcessorThrowableException()
            {
            }

            public ProcessorThrowableException(
                bool throwTaskMigration,
                bool throwStreamsException,
                bool throwKafkaException)
            {
                this.throwTaskMigration = throwTaskMigration;
                this.throwStreamsException = throwStreamsException;
                this.throwKafkaException = throwKafkaException;
            }
            
            public void Init(ProcessorContext<string, string> context)
            {
                context.Schedule(
                    TimeSpan.FromMilliseconds(10),
                    PunctuationType.STREAM_TIME,
                    (now) =>
                    {
                        if (throwTaskMigration)
                            throw new TaskMigratedException("Task migrated");
                        if (throwStreamsException)
                            throw new StreamsException("Aie aie aie");
                        if (throwKafkaException)
                            throw new KafkaException(ErrorCode.InvalidTimestamp);
                    });
            }

            public void Process(Record<string, string> record)
            {
            }

            public void Close()
            {
            }
        }
        
        private ConsumeResult<byte[], byte[]> CreateRecord(string topic, int partition, long offset, string key, string value, long ts)
        {
            return new ConsumeResult<byte[], byte[]>
            {
                Message = new Message<byte[], byte[]>
                {
                    Key = Encoding.UTF8.GetBytes(key),
                    Value = Encoding.UTF8.GetBytes(value),
                    Timestamp = new Timestamp(ts, TimestampType.CreateTime)
                },
                Topic = topic,
                Partition = partition,
                Offset = offset
            };
        }

        [Test]
        public void StandardSystemTimePunctuator()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-punctuator";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .Process(
                    new ProcessorBuilder<string, string>()
                        .Processor<MySystemProcessor>()
                        .Build());

            var topology = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);
            var restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());

            var storeChangelogReader =
                new StoreChangelogReader(config, restoreConsumer, "thread-0", new StreamMetricsRegistry());
            var taskCreator = new TaskCreator(topology.Builder, config, "thread-0", supplier, producer,
                storeChangelogReader, new StreamMetricsRegistry());
            var taskManager = new TaskManager(topology.Builder, taskCreator,
                supplier.GetAdmin(config.ToAdminConfig("admin")), consumer, storeChangelogReader);

            taskManager.CreateTasks(
                new List<TopicPartition> {
                    new("topic", 0)
                });
            Thread.Sleep(15);
            taskManager.TryToCompleteRestoration();
            
            Thread.Sleep(15);
            Assert.AreEqual(1, taskManager.Punctuate());
            Thread.Sleep(15);
            Assert.AreEqual(1, taskManager.Punctuate());
            
            taskManager.Close();
        }

        [Test]
        public void StandardStreamTimePunctuator()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-punctuator";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .Process(
                    new ProcessorBuilder<string, string>()
                        .Processor<MyEventProcessor>()
                        .Build());

            var topology = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);
            var restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());

            var storeChangelogReader =
                new StoreChangelogReader(config, restoreConsumer, "thread-0", new StreamMetricsRegistry());
            var taskCreator = new TaskCreator(topology.Builder, config, "thread-0", supplier, producer,
                storeChangelogReader, new StreamMetricsRegistry());
            var taskManager = new TaskManager(topology.Builder, taskCreator,
                supplier.GetAdmin(config.ToAdminConfig("admin")), consumer, storeChangelogReader);

            taskManager.CreateTasks(
                new List<TopicPartition> {
                    new("topic", 0)
                });
            taskManager.TryToCompleteRestoration();

            var task = taskManager.ActiveTaskFor(new("topic", 0));
            var now = DateTime.Now.GetMilliseconds();
            
            task.AddRecord(CreateRecord("topic", 0, 0, "key", "value1", now));
            task.Process();
            Assert.AreEqual(1, taskManager.Punctuate());
            task.AddRecord(CreateRecord("topic", 0, 1, "key", "value2",  now + 100));
            task.Process();
            Assert.AreEqual(1, taskManager.Punctuate());
            
            taskManager.Close();
        }

        [Test]
        public void CloseTaskScheduledTask()
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-punctuator";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .Process(
                    new ProcessorBuilder<string, string>()
                        .Processor<MyCloseProcessor>()
                        .Build());

            var topology = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);
            var restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());

            var storeChangelogReader =
                new StoreChangelogReader(config, restoreConsumer, "thread-0", new StreamMetricsRegistry());
            var taskCreator = new TaskCreator(topology.Builder, config, "thread-0", supplier, producer,
                storeChangelogReader, new StreamMetricsRegistry());
            var taskManager = new TaskManager(topology.Builder, taskCreator,
                supplier.GetAdmin(config.ToAdminConfig("admin")), consumer, storeChangelogReader);

            taskManager.CreateTasks(
                new List<TopicPartition> {
                    new("topic", 0)
                });
            taskManager.TryToCompleteRestoration();

            var task = taskManager.ActiveTaskFor(new("topic", 0));
            var now = DateTime.Now.GetMilliseconds();
            
            task.AddRecord(CreateRecord("topic", 0, 0, "key", "value1", now));
            task.Process();
            Assert.AreEqual(1, taskManager.Punctuate());
            task.AddRecord(CreateRecord("topic", 0, 1, "key", "value2",  now + 100));
            task.Process();
            Assert.AreEqual(1, taskManager.Punctuate());
            task.AddRecord(CreateRecord("topic", 0, 2, "key", "value3",  now + 200));
            task.Process();
            Assert.AreEqual(0, taskManager.Punctuate());
            
            taskManager.Close();
        }
        
        [Test]
        public void ScheduledTaskMigratedException()
        {
            CreateScheduledException(true, false, false);
        }
        
        [Test]
        public void ScheduledStreamsException()
        {
            CreateScheduledException(false, true, false);
        }
        
        [Test]
        public void ScheduledKafkaException()
        {
            CreateScheduledException(false, false, true);
        }


        private void CreateScheduledException(bool a, bool b, bool c)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-punctuator";

            var builder = new StreamBuilder();
            builder
                .Stream<string, string>("topic")
                .Process(
                    new ProcessorBuilder<string, string>()
                        .Processor<ProcessorThrowableException>(a, b, c)
                        .Build());

            var topology = builder.Build();

            var supplier = new SyncKafkaSupplier();
            var producer = supplier.GetProducer(config.ToProducerConfig());
            var consumer = supplier.GetConsumer(config.ToConsumerConfig(), null);
            var restoreConsumer = supplier.GetRestoreConsumer(config.ToConsumerConfig());

            var storeChangelogReader =
                new StoreChangelogReader(config, restoreConsumer, "thread-0", new StreamMetricsRegistry());
            var taskCreator = new TaskCreator(topology.Builder, config, "thread-0", supplier, producer,
                storeChangelogReader, new StreamMetricsRegistry());
            var taskManager = new TaskManager(topology.Builder, taskCreator,
                supplier.GetAdmin(config.ToAdminConfig("admin")), consumer, storeChangelogReader);

            taskManager.CreateTasks(
                new List<TopicPartition> {
                    new("topic", 0)
                });
            taskManager.TryToCompleteRestoration();

            var task = taskManager.ActiveTaskFor(new("topic", 0));
            var now = DateTime.Now.GetMilliseconds();
            
            task.AddRecord(CreateRecord("topic", 0, 0, "key", "value1", now));
            task.Process();
            
            if(a)
                Assert.Throws<TaskMigratedException>(() => taskManager.Punctuate());
            if(b || c)
                Assert.Throws<StreamsException>(() => taskManager.Punctuate());
            
            taskManager.Close();
        }
        
    }
}