#!/bin/bash

docker rm -f $(docker ps -aq) > /dev/null 2>&1
docker-compose -f environment/docker-compose.yml up -d

zookeeperContainerId=`docker ps -f name=zookeeper | tail -n 1 | awk '{print $1}'`
kafkaContainerId=`docker ps -f name=broker | tail -n 1 | awk '{print $1}'`

# Waiting zookeeper is UP
echo "Waiting zookeper ..."
test=true
while test
do
    ret=`echo ruok | docker exec -i ${zookeeperContainerId} nc localhost 2181 | awk '{print $1}'`
    sleep 1
    echo "Waiting zookeeper UP"
    if $ret == 'imok'
    then
        test=false
    fi
done

# Wait broker is UP
test=true
echo "Waiting kafka broker ..."
while test
do
    ret=`echo dump | docker exec -i ${zookeeperContainerId} nc localhost 2181 | grep brokers | wc -l`
    sleep 1
    echo "Waiting kafka UP"
    if $ret == 1
    then
        test=false
    fi
done

docker exec -i ${kafkaContainerId} kafka-topics --bootstrap-server broker:29092 --topic input --create --partitions 4 --replication-factor 1 > /dev/null 2>&1
docker exec -i ${kafkaContainerId} kafka-topics --bootstrap-server broker:29092 --topic output --create --partitions 4 --replication-factor 1 > /dev/null 2>&1
echo "Topics created"

echo "List all topics ..."
docker exec -i ${kafkaContainerId} kafka-topics --bootstrap-server broker:29092 --list

echo "Restore, build and run demo sample"
dotnet dev-certs https
dotnet restore
dotnet build -f net6.0 --no-restore

if [ $# -gt 0 ]
  then
     dotnet run -f net6.0 --project samples/sample-stream-demo/sample-stream-demo.csproj --no-build --no-restore
fi