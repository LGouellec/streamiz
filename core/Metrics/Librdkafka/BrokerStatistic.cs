using System.Collections.Generic;
using Newtonsoft.Json;

namespace Streamiz.Kafka.Net.Metrics.Librdkafka
{
 /// <summary>
    /// 
    /// </summary>
    public class BrokerStatistic
    {
        public enum BrokerState
        {
            INIT, DOWN, CONNECT, AUTH, APIVERSION_QUERY, AUTH_HANDSHAKE, UP, UPDATE
        }
        
        [JsonProperty(PropertyName = "name")]
        public string Name;

        [JsonProperty(PropertyName = "nodeid")]
        public int NodeId;

        [JsonProperty(PropertyName = "nodename")]
        public string NodeName;

        [JsonProperty(PropertyName = "source")]
        public string Source;

        [JsonProperty(PropertyName = "state")]
        public string _State;

        public BrokerState State
        {
            get
            {
                switch (_State)
                {
                    case "INIT": return BrokerState.INIT;
                    case "DOWN": return BrokerState.DOWN;
                    case "CONNECT": return BrokerState.CONNECT;
                    case "AUTH": return BrokerState.AUTH;
                    case "APIVERSION_QUERY": return BrokerState.APIVERSION_QUERY;
                    case "AUTH_HANDSHAKE": return BrokerState.AUTH_HANDSHAKE;
                    case "UP": return BrokerState.UP;
                    case "UPDATE": return BrokerState.UPDATE;
                    default: return BrokerState.UP;
                }
            }
        }

        [JsonProperty(PropertyName = "stateage")]
        public long TimeSinceLastBrokerStateChange; // Gauge

        [JsonProperty(PropertyName = "outbuf_cnt")]
        public long NumberOfRequestAwaitingTransmission; // Gauge

        [JsonProperty(PropertyName = "outbuf_msg_cnt")]
        public long NumberOfMessagesAwaitingTransmission; //Gauge

        [JsonProperty(PropertyName = "waitresp_cnt")]
        public long NumberOfRequestInFlight; // Gauge

        [JsonProperty(PropertyName = "waitresp_msg_cnt")]
        public long NumberOfMessagesInFlight; // Gauge

        [JsonProperty(PropertyName = "tx")]
        public long TotalNumberOfRequestSent;

        [JsonProperty(PropertyName = "txbytes")]
        public long TotalNumberOfBytesSent;

        [JsonProperty(PropertyName = "txerrs")]
        public long TotalNumberOfTransmissionErrors;

        [JsonProperty(PropertyName = "txretries")]
        public long TotalNumberOfRequestRetries;

        [JsonProperty(PropertyName = "req_timeout")]
        public long TotalNumberOfRequestTimeout;

        [JsonProperty(PropertyName = "rx")]
        public long TotalNumberOfResponsesReceived;

        [JsonProperty(PropertyName = "rxbytes")]
        public long TotalNumberOfBytesReceived;

        [JsonProperty(PropertyName = "rxerrs")]
        public long TotalNumberOfReceivedErrors;

        [JsonProperty(PropertyName = "rxcorriders")]
        public long TotalNumberOfUnmatchedCorrelationIds; // typically for time out requests

        [JsonProperty(PropertyName = "rxpartial")]
        public long TotalNumberOfPartialMessageSetsReceived;

        [JsonProperty(PropertyName = "req")]
        public Dictionary<string, string> RequestTypeCounter;

        [JsonProperty(PropertyName = "zbuf_grow")]
        public long TotalNumberOfDecompressionBufferSizeIncrases;

        [JsonProperty(PropertyName = "buf_grow")]
        public long TotalNumberOfBuffersizeIncreaes; // deprecated

        [JsonProperty(PropertyName = "wakeups")]
        public long BrokerThreadPollWakeups;

        [JsonProperty(PropertyName = "connects")]
        public long NumberOfConnectionAttemps; // Including successful, failed and name resolution failures

        [JsonProperty(PropertyName = "disconnects")]
        public long NumberOfDisconnects;

        [JsonProperty(PropertyName = "int_latency")]
        public WindowStatistic InternalQueueProducerLatency; // In microseconds

        [JsonProperty(PropertyName = "outbuf_latency")]
        public WindowStatistic InternalRequestQueueLatency; // In microseconds

        [JsonProperty(PropertyName = "rtt")]
        public WindowStatistic BrokerLatency; // Round-trip time in microseconds

        [JsonProperty(PropertyName = "throttle")]
        public WindowStatistic BrokerThrottlingTime; // in microsends

        [JsonProperty(PropertyName = "toppars")]
        public Dictionary<string, Toppars> PartitionsHandled;

        public BrokerStatistic()
        {
            PartitionsHandled = new Dictionary<string, Toppars>();
        }
    }

    public class Toppars
    {
        [JsonProperty(PropertyName = "topic")]
        public string TopicName;

        [JsonProperty(PropertyName = "partition")]
        public int PartitionId;
    }
}