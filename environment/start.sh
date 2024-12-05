#!/bin/bash

curl -i -X PUT http://localhost:8083/connectors/datagen_order/config \
     -H "Content-Type: application/json" \
     -d '{
        "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
        "kafka.topic": "input",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "max.interval": 100,
        "iterations": 10000000,
        "tasks.max": "1",
        "schema.filename": "/home/appuser/order.avsc",
        "schema.keyfield": "name"
    }'

# curl -i -X PUT http://localhost:8083/connectors/datagen_order/pause 
# curl -i -X PUT http://localhost:8083/connectors/datagen_order/resume 
# curl -X DELETE http://localhost:8083/connectors/datagen_order