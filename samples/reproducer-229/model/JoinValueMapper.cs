using Reproducer229.Model;
using Streamiz.Kafka.Net.Stream;

namespace Reproducer229.Model{
    
internal class JoinValueMapper : IValueJoiner<Ticket, TicketDetails, Progression>
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
}