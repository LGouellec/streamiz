using System;
using System.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests
{
    public class Reproducer229Tests
    {
        #region Inner class

        private class JoinValueMapper : IValueJoiner<Ticket, TicketDetails, Progression>
        {
            public Progression Apply(Ticket ticket1Value, TicketDetails ticket2Value)
            {
                return new Progression
                {
                    id = ticket1Value.id,
                    user = ticket2Value.user
                };
            }
        }

        private class Progression
        {
            public string id { get; set; }
            public string user { get; set; }
        }

        private class Ticket {
            public string id { get; set; }
        }

        private class TicketDetails
        {
            public string id { get; set; }
            public string user { get; set; }
        }
        
        #endregion
        
        private void StreamStreamJoinBase(bool persistent)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>
            {
                ApplicationId = "test-stream-stream-join",
                StateDir = "./state"
            };

            StreamBuilder builder = new StreamBuilder();

            var ticket1Stream = builder
                .Stream<string, Ticket, StringSerDes, JsonSerDes<Ticket>>("tickets");
            var lotteryTicketDetailsStream = builder
                .Stream<string, TicketDetails, StringSerDes, JsonSerDes<TicketDetails>>("tickets-details");
            
            var props = StreamJoinProps.With<string, Ticket, TicketDetails>(
                persistent ? 
                    Streamiz.Kafka.Net.State.Stores.PersistentWindowStore("tickets-store", TimeSpan.FromDays(4),
                        TimeSpan.FromDays(4), 3600000, true) :
                    Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("tickets-store", TimeSpan.FromDays(4),
                    TimeSpan.FromDays(4), true),
                persistent ? 
                    Streamiz.Kafka.Net.State.Stores.PersistentWindowStore("tickets-details-store", TimeSpan.FromDays(4),
                        TimeSpan.FromDays(4), 3600000, true) :
                    Streamiz.Kafka.Net.State.Stores.InMemoryWindowStore("tickets-details-store", TimeSpan.FromDays(4),
                    TimeSpan.FromDays(4), true));
            
            var joinValueMapper = new JoinValueMapper();
            var joinWindowOptions = JoinWindowOptions.Of(TimeSpan.FromDays(2));

             ticket1Stream
                .Join(lotteryTicketDetailsStream, joinValueMapper, joinWindowOptions, props)
                .To("output", new StringSerDes(), new JsonSerDes<Progression>());

            Topology t = builder.Build();

            using (var driver = new TopologyTestDriver(t, config))
            {
                var inputTicket = driver.CreateInputTopic<string, Ticket, StringSerDes, JsonSerDes<Ticket>>("tickets");
                var inputTicketDetails = driver.CreateInputTopic<string, TicketDetails, StringSerDes, JsonSerDes<TicketDetails>>("tickets-details");
                
                var outputTopic = driver.CreateOuputTopic<string, string>("output");
                
                inputTicketDetails.PipeInput("ticket-2", new TicketDetails{id = "2", user = "User2"}, 1676642917004.FromMilliseconds());
                inputTicketDetails.PipeInput("ticket-1", new TicketDetails{id = "1", user = "User1"}, 1676642916975.FromMilliseconds());
                inputTicketDetails.PipeInput("ticket-3", new TicketDetails{id = "3", user = "User3"}, 1676642917004.FromMilliseconds());
                inputTicketDetails.PipeInput("ticket-2", new TicketDetails{id = "2", user = "User2"}, 1676642917004.FromMilliseconds());
                inputTicketDetails.PipeInput("ticket-1", new TicketDetails{id = "1", user = "User1"}, 1676642917004.FromMilliseconds());
                inputTicketDetails.PipeInput("ticket-3", new TicketDetails{id = "3", user = "User3"}, 1676642917006.FromMilliseconds());
                inputTicketDetails.PipeInput("ticket-4", new TicketDetails{id = "4", user = "User4"}, 1676642917006.FromMilliseconds());
                inputTicketDetails.PipeInput("ticket-5", new TicketDetails{id = "5", user = "User5"}, 1676642917006.FromMilliseconds());
                inputTicketDetails.PipeInput("ticket-4", new TicketDetails{id = "4", user = "User4"}, 1676642917006.FromMilliseconds());
                inputTicketDetails.PipeInput("ticket-5", new TicketDetails{id = "5", user = "User5"}, 1676642917007.FromMilliseconds());
                
                inputTicket.PipeInput("ticket-2", new Ticket{id = "2"}, 1676642921410.FromMilliseconds());
                inputTicket.PipeInput("ticket-1", new Ticket{id = "1"}, 1676642921364.FromMilliseconds());
                inputTicket.PipeInput("ticket-3", new Ticket{id = "3"}, 1676642921410.FromMilliseconds());
                inputTicket.PipeInput("ticket-4", new Ticket{id = "4"}, 1676642921411.FromMilliseconds());
                inputTicket.PipeInput("ticket-5", new Ticket{id = "5"}, 1676642921411.FromMilliseconds());
                
                var records = outputTopic.ReadKeyValueList();
                Assert.IsNotNull(records);
                Assert.AreEqual(10, records.Count());
            }
        }
        
        [Test]
        public void StreamStreamJoinInMemory()
        {
            StreamStreamJoinBase(false);
        }
        
        [Test]
        public void StreamStreamJoinPersistent()
        {
            StreamStreamJoinBase(true);
        }
    }
}