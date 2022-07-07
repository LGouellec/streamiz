# Asynchronous processing

Often, you want to create a stream processing application, process data from Kafka topic to another Kafka topic, but in the middle of your topology, you need to get data from another tier system (oracle db, mongo db, http rest api or whatever). 

Since `1.4.0` release, Streamiz release multiple asynchronous processors :
- MapAsync(...)
- MapValuesAsync(...)
- FlatMapAsync(...)
- FlatMapValuesAsync(...)
- ForEachAsync(...)

These processors use the pattern request/reponse to satisfy the asynchronous request with some retry policy options.

## Example

**Use case: Enrich data from a mongodb instance**

- Create a directory which names : `mongo-init` and a file `init.js` inside 

``` javascript
db = db.getSiblingDB('streamiz');
db.createCollection('adress');
db.adress.insert({ "address": { "city": "Paris", "zip": "123" }, "name": "Mike", "phone": "1234" });
db.adress.insert({ "address": { "city": "Marsel", "zip": "321" }, "name": "Helga", "phone": "4321" });
```

- Use this `docker-compose` file 

```yaml
---
version: '2'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.1.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
      KAFKA_OPTS: "-Dzookeeper.4lw.commands.whitelist=*"

  broker:
    image: confluentinc/cp-server:7.1.0
    hostname: broker
    container_name: broker
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_METRIC_REPORTERS: io.confluent.metrics.reporter.ConfluentMetricsReporter
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_CONFLUENT_SCHEMA_REGISTRY_URL: http://schema-registry:8081
      CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS: broker:29092
      CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS: 1
      CONFLUENT_METRICS_ENABLE: 'true'
      CONFLUENT_SUPPORT_CUSTOMER_ID: 'anonymous'

  schema-registry:
    image: confluentinc/cp-schema-registry:7.1.0
    hostname: schema-registry
    container_name: schema-registry
    depends_on:
      - broker
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'broker:29092'
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081

  akhq:
    image: tchiotludo/akhq:latest
    environment:
      AKHQ_CONFIGURATION: |
        akhq:
          server:
            access-log:
              enabled: false
          connections:
            docker-kafka-server:
              properties:
                bootstrap.servers: "broker:29092"
              schema-registry:
                type: "confluent"
                url: "http://schema-registry:8081"
    ports:
      - 8082:8080
    links:
      - broker

  mongo:
    image: 'mongo'
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: admin
    volumes:
      - ./mongo-init/:/docker-entrypoint-initdb.d/:ro
    ports:
      - "27017:27017"


```

- Create the input topic

```bash
docker-compose exec broker kafka-topics --bootstrap-server broker:29092 --topic input --create --partitions 4
```

- Create your streamiz topology

```csharp
    internal class Program
    {
        public class Address
        {
            public string city { get; set; }
            public string zip { get; set; }
        }

        public class Person
        {
            public ObjectId _id { get; set; }
            public Address address { get; set; }
            public string name { get; set; }
            public string phone { get; set; }
        }

        
        public static async Task Main(string[] args)
        {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "test-app2";
            config.BootstrapServers = "localhost:9092";
            config.AutoOffsetReset = AutoOffsetReset.Earliest;

            StreamBuilder builder = new StreamBuilder();

            var client = new MongoClient(
                "mongodb://admin:admin@localhost:27017"
            );
            var database = client.GetDatabase("streamiz");

            builder
                .Stream<string, string>("input")
                .MapValuesAsync(async (record, _) => {
                    var persons = await database
                        .GetCollection<Person>("adress")
                        .FindAsync((p) => p.name.Equals(record.Key))
                        .Result.ToListAsync();
                    return persons.FirstOrDefault()?.address.city;
                })
                .To("person-city");
            
            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);
            
            Console.CancelKeyPress += (o, e) => stream.Dispose();

            await stream.StartAsync();
        }
    }
```

- Subscribe output topic `person-city`

```bash
docker-compose exec broker kafka-console-consumer --bootstrap-server broker:29092 --topic person-city --property print.key=true --property --print.value=true --from-beginning --max-messages 1
```

- Open a new terminal and produce one message in the source topic

```bash
docker-compose exec broker kafka-console-producer --bootstrap-server broker:29092 --topic input --property parse.key=true --property key.separator=:

> Mike:Mike
```

- Assert the kafka console consumer output

```bash
> Mike    Paris
```

- Here the schema of this topology

![async-topology](./assets/async-topology.png)