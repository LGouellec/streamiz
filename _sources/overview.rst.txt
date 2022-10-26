Overview
=============================================

.. image:: assets/logo-kafka-stream-net.png
   :width: 200
   :align: center

**Streamiz.Kafka.Net** is a .NET stream processing library for Apache Kafka (TM).
**Streamiz.Kafka.Net** is cross-platforms and based on .Net Standard 2.0, .NET 5 and .NET 6.

With a few lines of code, you may to create your first .NET streamiz application.

Nuget package
----------------------

.. code-block:: shell

    dotnet add package Streamiz.Kafka.Net


Tutorial: First streamiz application
----------------------------------------

This tutorial will describe all the steps required to create a first streamiz application with a simple case :

.. code-block:: csharp

    static async Task Main(string[] args)
    {
        // Stream configuration
        var config = new StreamConfig<StringSerDes, StringSerDes>();
        config.ApplicationId = "test-app";
        config.BootstrapServers = "localhost:9092";

        StreamBuilder builder = new StreamBuilder();

        // Stream "test" topic with filterNot condition and persist in "test-output" topic.
        builder.Stream<string, string>("test")
            .FilterNot((k, v) => v.Contains("test"))
            .To("test-output");

        // Create a table with "test-ktable" topic, and materialize this with in memory store named "test-store"
        builder.Table("test-ktable", InMemory.As<string, string>("test-store"));

        // Build topology
        Topology t = builder.Build();

        // Create a stream instance with toology and configuration
        KafkaStream stream = new KafkaStream(t, config);

        // Subscribe CTRL + C to quit stream application
        Console.CancelKeyPress += (o, e) =>
        {
            stream.Dispose();
        };

        // Start stream instance with cancellable token
        await stream.StartAsync();
    }