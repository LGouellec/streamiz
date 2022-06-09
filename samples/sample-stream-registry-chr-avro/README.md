Steps:

copy Dockerfile next to sln
copy docker-compose.yml file next to sln

run "docker-compose up -d"

Now the sample application and the confluent kafka setup are running!
Note: The sample app inside the docker container cannot access the kafka broker. But for testing, the code should be updated to use:
	BootstrapServers = "broker:29092",
	SchemaRegistryUrl = "http://schema-registry:8081",

You can visit http://localhost:9021 to check Confluent's control center, but give it a minute or two. :)
You can run the sample app directly since now it has access to Confluent.

The sample app creates the necessary topics, generates the AVRO schema for them and publishes it to Schema Registry.
Note: The models used for topic messages don't contain the schema information, we consult directly with Schema Registry.

Inside the sample app we build the topology and perform a join test as follows:
	* we publish a message to the "product" topic
	* we publish a message to the "orders" topic
	* We subscribe to the "orders-output" topic which soon will be populated with the product and order info after joining the two topics

The sample app waits continuously for other messages so other tests can be performed.

Make sure to clean up once testing is done:
	* docker-compose down -v

