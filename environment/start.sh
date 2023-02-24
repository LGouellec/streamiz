#!/bin/bash

curl -i -X PUT http://localhost:8083/connectors/datagen_local_01/config \
     -H "Content-Type: application/json" \
     -d '{
            "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": false,
            "kafka.topic": "shoes",
            "quickstart": "shoes",
            "max.interval": 100,
            "iterations": 10000000,
            "tasks.max": "1"
        }'

curl -i -X PUT http://localhost:8083/connectors/datagen_local_02/config \
     -H "Content-Type: application/json" \
     -d '{
            "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": false,
            "kafka.topic": "orders",
            "quickstart": "shoe_orders",
            "max.interval": 100,
            "iterations": 10000000,
            "tasks.max": "1"
        }'