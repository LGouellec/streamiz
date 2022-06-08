Steps:

copy Dockerfile next to sln
copy docker-compose.yml file next to sln

run "docker-compose up -d"

run "docker build -t schema-registry-test . "
run "docker run -it --rm schema-registry-test"

Now both the sample application and the confluent kafka setup are running!
You can visit http://localhost:9021 to check Confluent's control center, but give it a minute or two. :)

The sample app creates the necessary topics, generates the AVRO schema for them and publishes it to Schema Registry.
Note: The models used for topic messages don't contain the schema information, we consult directly with Schema Registry.

Inside the sample app we build the topology and perform a join test as follows:
	* we publish a message to the "product" topic
	* we publish a message to the "orders" topic
	* We subscribe to the "orders-output" topic which soon will be populated with the product and order info after joining the two topics

The sample app waits continuously for other messages so other tests can be performed.

Make sure to clean up once testing is done:
	* docker-compose down -v
	* docker stop schema-registry-test
	* docker rm schema-registry-test

