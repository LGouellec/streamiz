Welcome to Streamiz.Kafka.Net 🚀
=============================================

.. image:: assets/logo-kafka-stream-net.png
   :width: 200
   :align: center


**Streamiz.Kafka.Net** is a .NET stream processing library for Apache Kafka (TM).

.. note::
   The source code is available on `Github <https://github.com/LGouellec/kafka-streams-dotnet>`_. 

It's allowed to develop .NET applications that transform input Kafka topics into output Kafka topics. 
It's a rewriting inspired by `Kafka Streams <https://github.com/apache/kafka>`_. 

Streamiz.Kafka.Net aims to provide the same functionality as `Kafka Streams <https://github.com/apache/kafka>`_. 
So you can found documentation here :

* `Confluent Kafka Streams <https://docs.confluent.io/current/streams/index.html>`_
* `Apache Kafka Streams <https://kafka.apache.org/documentation/streams>`_

Starting from scratch
=============================================

Here is the easiest way to create a first sync, from scratch : 
 
* Create a **.Net Standard 2.0, .NET 5 or .NET 6** compatible project. For more information, please find help here `net-standard <https://docs.microsoft.com/fr-fr/dotnet/standard/net-standard#net-implementation-support>`_
* Add the nuget package Streamiz.Kafka.Net
* Please retrieve your kafka cluster connection information

Add this code ::

   static async Task Main(string[] args)
   {     
      var config = new StreamConfig<StringSerDes, StringSerDes>();
      config.ApplicationId = "test-app";
      config.BootstrapServers = "localhost:9092";
      
      StreamBuilder builder = new StreamBuilder();

      builder.Stream<string, string>("test")
         .FilterNot((k, v) => v.Contains("key1"))
         .To("test-output");

      Topology t = builder.Build();
      KafkaStream stream = new KafkaStream(t, config);

      Console.CancelKeyPress += (o, e) => {
         stream.Dispose();
      };

      await stream.StartAsync();
   }


You’re done !

Need help
=============================================

`@LGouellec <https://twitter.com/LGouellec>`_


.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Streamiz.Kafka.Net

   overview
   stream-configuration
   threading-model
   stateless-processors
   stateful-processors
   stores
   topology-test-driver
   monitoring
   async-processing
   processor-api