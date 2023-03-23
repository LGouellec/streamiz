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
    public static class CloudEventSerDesConfig
    {
        /// <summary>
        /// Key for override the cloud event formatter. The value for this configuration should be an instance of <see cref="CloudNative.CloudEvents.CloudEventFormatter"/>.
        /// </summary>
        public static readonly string CloudEventFormatter = "cloudevent.formatter";
        
        /// <summary>
        /// Key for override the cloud event deserializer use to convert a <see cref="CloudEvent"/> message to your business object. The value for this configuration should be an instance of Func&lt;CloudEvent,T&gt;.
        /// </summary>
        public static readonly string CloudEventDeserializer = "cloudevent.deserializer";
        
        /// <summary>
        /// Key for override the cloud event content mode use. The value for this configuration should be an instance of <see cref="ContentMode"/>.
        /// </summary>
        public static readonly string CloudEventContentMode = "cloudevent.content.mode";
        
        /// <summary>
        /// Key for override the cloud event exporter to export the event id from your business object. The value for this configuration should be an instance of Func&lt;T,string&gt;.
        /// </summary>
        public static readonly string CloudEventExportId = "cloudevent.exporter.event.id";
    }
    

    /// <summary>
    /// CloudEvent SerDes without schema for <typeparamref name="T"/>. This serdes is only available to serialize/deserialize the value.
    /// This serdes will use a cloud event formatter to format the value before wrapping this one with specific header like id, specversion, type, source.
    /// Per default, the cloud event serdes will use the <see cref="CloudNative.CloudEvents.SystemTextJson.JsonEventFormatter"/>, the content mode (<see cref="ContentMode"/>) will be encoded in Binary.
    /// Each record will assign an event id auto-generated using a new <see cref="Guid"/>, you can override this behavior using the constructor parameter or the <see cref="IStreamConfig"/> via this helper class <see cref="CloudEventSerDesConfig"/>.
    /// </summary>
    public class CloudEventSerDes<T> : AbstractSerDes<T>
        where T : class
    {
        private CloudEventFormatter formatter;
        private Func<CloudEvent, T> deserializer;
        private ContentMode mode;
        private Func<T, string> exportCloudEventId;

        /// <summary>
        /// Empty constructor, will use the <see cref="JsonEventFormatter"/>, <see cref="ContentMode.Binary"/> and deserialize the content using <see cref="JsonSerializer"/>.
        /// </summary>
        public CloudEventSerDes()
            : this(
                new JsonEventFormatter(), 
            @event => JsonSerializer.Deserialize<T>(JsonDocument.Parse(@event?.Data?.ToString())),
                ContentMode.Binary)
        { }

        /// <summary>
        /// Constructor where you can set your own formatter, deserializer and content mode.
        /// </summary>
        /// <param name="formatter">Cloud event formatter use to encode and decode the Kafka Message to a Cloud event message and vice-versa.</param>
        /// <param name="deserializer">Deserializer use to deserialize the cloud event data to a <typeparamref name="T"/> object </param>
        /// <param name="mode">Content mode use by the formatter</param>
        public CloudEventSerDes(
            CloudEventFormatter formatter, 
            Func<CloudEvent, T> deserializer,
            ContentMode mode
            )
        : this(formatter, deserializer, mode, _ => Guid.NewGuid().ToString())
        { }
        
        /// <summary>
        /// Constructor where you can set your own formatter, deserializer, content mode and the exporter for the cloud event id.
        /// </summary>
        /// <param name="formatter">Cloud event formatter use to encode and decode the Kafka Message to a Cloud event message and vice-versa.</param>
        /// <param name="deserializer">Deserializer use to deserialize the cloud event data to a <typeparamref name="T"/> object </param>
        /// <param name="mode">Content mode use by the formatter</param>
        /// <param name="exportCloudEventId">Export the cloud event id for each <typeparamref name="T"/> record</param>
        public CloudEventSerDes(
            CloudEventFormatter formatter, 
            Func<CloudEvent, T> deserializer, 
            ContentMode mode,
            Func<T, string> exportCloudEventId)
        {
            Validation.CheckNotNull(formatter, nameof(formatter));
            Validation.CheckNotNull(deserializer, nameof(deserializer));
            Validation.CheckNotNull(exportCloudEventId, nameof(exportCloudEventId));
            
            this.formatter = formatter;
            this.deserializer = deserializer;
            this.mode = mode;
            this.exportCloudEventId = exportCloudEventId;
        }
        
        
        /// <summary>
        /// Convert <typeparamref name="T"/> <code>data</code> into a byte array with the cloud event formatter
        /// </summary>
        /// <param name="data">object data</param>
        /// <param name="context">serialization context</param>
        /// <returns>serialized bytes</returns>
        public override byte[] Serialize(T data, SerializationContext context)
        {
            if (context.Component == MessageComponentType.Key)
                throw new StreamsException("This serdes is only accessible for the value");

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
            
            var tmpMessage = cloudEvent.ToKafkaMessage(mode, formatter);
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

        /// <summary>
        /// Deserialize a record value from a byte array into <typeparamref name="T"/> value
        /// </summary>
        /// <param name="data">serialized bytes.</param>
        /// <param name="context">serialization context</param>
        /// <returns>deserialized <typeparamref name="T"/> using data; may be null</returns>
        public override T Deserialize(byte[] data, SerializationContext context)
        {
            if (context.Component == MessageComponentType.Key)
                throw new StreamsException("This serdes is only accessible for the value");

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

            return deserializer(cloudEvent);
        }

        /// <summary>
        /// Initialize method with a current context which contains <see cref="IStreamConfig"/>.
        /// Can be used to initialize the serdes according to some parameters present in the configuration.
        /// Please use this helper class <see cref="CloudEventSerDesConfig"/> for the key configuration.
        /// </summary>
        /// <param name="context">SerDesContext with stream configuration</param>
        public override void Initialize(SerDesContext context)
        {
            if (!isInitialized)
            {
                CloudEventFormatter eventFormatter = context.Config.Get(CloudEventSerDesConfig.CloudEventFormatter);
                String eventContentMode = context.Config.Get(CloudEventSerDesConfig.CloudEventContentMode)?.ToString();
                var eventDeserializer = context.Config.Get(CloudEventSerDesConfig.CloudEventDeserializer);
                var eventExporterId = context.Config.Get(CloudEventSerDesConfig.CloudEventExportId);
                
                formatter ??= eventFormatter;
                deserializer ??= (Func<CloudEvent, T>)eventDeserializer;
                if (!String.IsNullOrEmpty(eventContentMode) 
                    && Enum.TryParse(eventContentMode, out ContentMode newMode))
                    mode = newMode;
                exportCloudEventId ??= (Func<T, string>)eventExporterId;
            }
            base.Initialize(context);
        }
    }
}