#!/bin/bash

echo "${red}Stop and remove all docker container existing${reset}"
docker rm -f $(docker ps -aq) > /dev/null 2>&1

docker-compose build
docker-compose up -d zookeeper broker akhq

kafkaContainerId=`docker ps -f name=broker | tail -n 1 | awk '{print $1}'`

docker exec ${kafkaContainerId} kafka-topics --bootstrap-server broker:29092 --topic tickets --create --partitions 3 --replication-factor 1 > /dev/null 2>&1
echo "${green} Topic tickets created"
docker exec ${kafkaContainerId} kafka-topics --bootstrap-server broker:29092 --topic tickets-details --create --partitions 3 --replication-factor 1 > /dev/null 2>&1
echo "${green} Topic tickets-details created"
docker exec ${kafkaContainerId} kafka-topics --bootstrap-server broker:29092 --topic output --create --partitions 3 --replication-factor 1 > /dev/null 2>&1
echo "${green} Topic output created"


cat ./data/tickets | docker exec -i ${kafkaContainerId} kafka-console-producer --bootstrap-server broker:29092 --topic tickets --property parse.key=true --property key.separator=\| --property key.serializer=org.apache.kafka.common.serialization.StringSerializer
cat ./data/tickets-details | docker exec -i ${kafkaContainerId} kafka-console-producer --bootstrap-server broker:29092 --topic tickets-details --property parse.key=true --property key.separator=\| --property key.serializer=org.apache.kafka.common.serialization.StringSerializer

docker-compose up -d 

docker exec -it ${kafkaContainerId} kafka-console-consumer --bootstrap-server broker:29092 --topic output --from-beginning --property print.key=true --property key.separator=" : " --key-deserializer org.apache.kafka.common.serialization.StringDeserializer --value-deserializer org.apache.kafka.common.serialization.StringDeserializer