using System;
using System.Text.Json;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Core;
using CloudNative.CloudEvents.Kafka;
using CloudNative.CloudEvents.SystemTextJson;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.SerDes.CloudEvents
{
    /// <summary>
    /// Only for value
    /// </summary>
    public class CloudEventSerDes<T> : AbstractSerDes<T>
        where T : class
    {
        private readonly CloudEventFormatter formatter;
        private readonly Func<CloudEvent, T> deserializer;
        private readonly Func<T, string> exportCloudEventId;

        public CloudEventSerDes()
            : this(
                new JsonEventFormatter(), 
            @event => JsonSerializer.Deserialize<T>(JsonDocument.Parse(@event?.Data?.ToString())))
        { }

        public CloudEventSerDes(
            CloudEventFormatter formatter, 
            Func<CloudEvent, T> deserializer)
        : this(formatter, deserializer, _ => Guid.NewGuid().ToString())
        { }
        
        public CloudEventSerDes(
            CloudEventFormatter formatter, 
            Func<CloudEvent, T> deserializer, 
            Func<T, string> exportCloudEventId)
        {
            Validation.CheckNotNull(formatter, nameof(formatter));
            Validation.CheckNotNull(deserializer, nameof(deserializer));
            Validation.CheckNotNull(exportCloudEventId, nameof(exportCloudEventId));
            
            this.formatter = formatter;
            this.deserializer = deserializer;
            this.exportCloudEventId = exportCloudEventId;
        }
        
        public override byte[] Serialize(T data, SerializationContext context)
        {
            if (context.Component == MessageComponentType.Key)
                throw new StreamsException("This serdes is only accessible for the value serdes");

            if (data == null)
                return null;
            
            var cloudEvent = new CloudEvent()
            {
                Id = exportCloudEventId(data),
                Type = typeof(T).FullName,
                Data = data,
                Source = new Uri("topic://" +  context.Topic),
                DataContentType = null
            };
            cloudEvent.DataContentType = formatter.GetOrInferDataContentType(cloudEvent);
            
            var tmpMessage = cloudEvent.ToKafkaMessage(ContentMode.Binary, formatter);
            UpdateCurrentHeader(tmpMessage, context);
            return tmpMessage.Value;
        }

        private void UpdateCurrentHeader(Message<string?,byte[]> tmpMessage, SerializationContext context)
        {
            foreach(var header in tmpMessage.Headers)
            {
                if (context.Headers.TryGetLastBytes(header.Key, out byte[] lastHeader))
                    context.Headers.Remove(header.Key);
                
                context.Headers.Add(header.Key, header.GetValueBytes());
            }
        }

        public override T Deserialize(byte[] data, SerializationContext context)
        {
            if (context.Component == MessageComponentType.Key)
                throw new StreamsException("This serdes is only accessible for the value serdes");

            if (data == null)
                return null;
            
            var tmpMessage = new Message<string?, byte[]>
            {
                Headers = context.Headers,
                Key = null, // ignore in the extensions method
                Value = data
            };

            if (!tmpMessage.IsCloudEvent())
                throw new InvalidOperationException("The message is not a CloudEvent record." +
                                                    " Some kafka headers are needed to consider this message as a CloudEvent record." +
                                                    " Please refer to the kafka protocol binding in the cloudevents specs.");

            var cloudEvent = tmpMessage.ToCloudEvent(formatter);

            return cloudEvent.Data switch
            {
                JsonElement => deserializer(cloudEvent),
                T eventData => eventData,
                _ => null
            };
        }
    }
}