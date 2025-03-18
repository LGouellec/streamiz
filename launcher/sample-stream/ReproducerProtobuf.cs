using System;
using System.Threading.Tasks;
using CloudNative.CloudEvents;
using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.SerDes.CloudEvents;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace sample_stream;

public class ReproducerProtobuf
{
    public class dim_price
    {
        public int id { get; set; }
        public float price { get; set; }
    }

    public class dim_produit
    {
        public int id { get; set; }
        public string name { get; set; }
    }

    public class product
    {
        public int id { get; set; }
        public string name { get; set; }
        public float price { get; set; }
    }


    public async Task Test()
    {
        var config = new StreamConfig<StringSerDes, StringSerDes>();
        config.BootstrapServers = "localhost:9092";
        config.AutoOffsetReset = AutoOffsetReset.Earliest;
        config.ApplicationId = "test-test-driver-app";
        config[CloudEventSerDesConfig.CloudEventContentMode] = ContentMode.Structured;

        StreamBuilder builder = new StreamBuilder();

        var dim_price = builder.Table<string, dim_price, StringSerDes, CloudEventSerDes<dim_price>>("dim_price");
        var dim_produit =
            builder.Table<string, dim_produit, StringSerDes, CloudEventSerDes<dim_produit>>("dim_produit");

        var joinedTable = dim_produit
            .Join(dim_price,
                (product, price) =>
                    new product() { id = product.id, name = product.name, price = price.price },
                RocksDb.As<string, product>()
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new CloudEventSerDes<product>()));

        joinedTable
            .ToStream()
            // if you need to sink your data into another format AVRO, JSON, PROTOBUF, feel free to change the value serdes
            .To<StringSerDes, CloudEventSerDes<product>>("product");

        Topology t = builder.Build();
        
        var streamApp = new KafkaStream(t, config);
        
        Console.CancelKeyPress += (_, _) => { streamApp.Dispose(); };
        
        await streamApp.StartAsync();
    }
}