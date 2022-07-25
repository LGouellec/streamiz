using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;

namespace Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 2)
            {
                string topic = args[0];
                string mode = args[1];

                var config = new ConsumerConfig
                {
                    GroupId = "test-app",
                    BootstrapServers = "localhost:9092",
                    Debug = "generic, broker, topic, metadata, consumer"
                };

                var builder = new ConsumerBuilder<string, string>(config);

                if (mode.ToUpper().Equals("ASSIGN"))
                    Assign(builder, config.BootstrapServers, config.GroupId, topic);
                else if (mode.ToUpper().Equals("CONSUME"))
                    Consume(builder, topic);
            }
        }

        static void Assign(ConsumerBuilder<string, string> builder, string bootstrap, string groupId, string topic)
        {
            var offsets = new List<TopicPartitionOffset>
            {
                new TopicPartitionOffset(new TopicPartition(topic, 0), 2),
                new TopicPartitionOffset(new TopicPartition(topic, 1), 1),
            };

            // Check if none consumer is register for this consumer group
            var adminClientConfig = new AdminClientConfig();
            adminClientConfig.BootstrapServers = bootstrap;
            var adminClientBuilder = new AdminClientBuilder(adminClientConfig);

            using (var adminClient = adminClientBuilder.Build())
            {
                var groupInfo = adminClient.ListGroup(groupId, TimeSpan.FromSeconds(10));
                if (groupInfo.Members.Count > 0)
                {
                    Console.WriteLine($"Error consumers already exist in this consumer group {groupId}");
                    foreach (var member in groupInfo.Members)
                        Console.WriteLine(
                            $"Member {member.MemberId} (client.id:{member.ClientId}#client.host:{member.ClientHost}) =" +
                            $" Assigment {DecodeMemberAssignment(member.MemberAssignment)}");
                    return;
                }
            }

            using (var consumer = builder.Build())
            {
                consumer.Commit(offsets);
            }
        }

        static void Consume(ConsumerBuilder<string, string> builder, string topic)
        {
            bool run = true;

            var consumer = builder.Build();
            consumer.Subscribe(topic);

                Console.CancelKeyPress += (p, e) =>
                {
                    run = false;
                    consumer.Unsubscribe();
                    consumer.Close();
                    consumer.Dispose();
                };

                while (run)
                {
                    consumer.Consume(TimeSpan.FromSeconds(1));
                }
        }

        // From : https://github.com/confluentinc/confluent-kafka-dotnet/issues/1307
        static string DecodeMemberAssignment(byte[] b)
        {
            /*
            https://kafka.apache.org/protocol
            STRING	Represents a sequence of characters. First the length N is given as an INT16. Then N bytes follow which are the UTF-8 encoding of the character sequence. Length must not be negative.
            INT16	Represents an integer between -2^15 and 2^15-1 inclusive. The values are encoded using two bytes in network byte order (big-endian).
            INT32	Represents an integer between -2^31 and 2^31-1 inclusive. The values are encoded using four bytes in network byte order (big-endian).
            BYTES	Represents a raw sequence of bytes. First the length N is given as an INT32. Then N bytes follow.
        
            https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol	
            MemberAssignment => Version PartitionAssignment
              Version => int16
              PartitionAssignment => [Topic [Partition]]
                Topic => string
                Partition => int32
              UserData => bytes	
              
            Note: [] probably denotes a sequence of same type items, begining with Int32 of the # of such items
            */

            UTF8Encoding enc = new UTF8Encoding();
            StringBuilder s = new StringBuilder();
            try
            {
                short version = SwapEndianness(BitConverter.ToInt16(b, 0));
                int num_topic_assignments = SwapEndianness(BitConverter.ToInt32(b, 2));
                int i = 6;
                for (int t = 0; t < num_topic_assignments; t++)
                {
                    short topic_len = SwapEndianness(BitConverter.ToInt16(b, i));
                    byte[] str = new byte[topic_len];
                    Array.Copy(b, i + 2, str, 0, topic_len);
                    string topic = enc.GetString(str);
                    i += (topic_len + 2);
                    int num_partition = SwapEndianness(BitConverter.ToInt32(b, i));
                    if (s.Length > 0) s.Append($"; ");
                    s.Append($"{topic}: ");
                    for (int j = 0; j < num_partition; j++)
                    {
                        i += 4;
                        s.Append(
                            $"{SwapEndianness(BitConverter.ToInt32(b, i))}{(j < num_partition - 1 ? "," : "")}");
                    }
                }

                return s.ToString();
            }
            catch
            {
                return "";
            }
        }

        static int SwapEndianness(int value)
        {
            var b1 = (value >> 0) & 0xff;
            var b2 = (value >> 8) & 0xff;
            var b3 = (value >> 16) & 0xff;
            var b4 = (value >> 24) & 0xff;
            return b1 << 24 | b2 << 16 | b3 << 8 | b4 << 0;
        }

        static Int16 SwapEndianness(Int16 i)
        {
            return (Int16) ((i << 8) + (i >> 8));
        }
    }
}