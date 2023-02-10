You can find here a solution (src/Streamiz.Demo.sln), that has 2 projects

- src/Streamiz.Demo/Streamiz.Demo.csproj: A sample dotnet app with
    - an endpoint on localhost:8060/ that just replies Hello
    - A hosted background service with the KStream app

- src/Streamiz.Tests/Streamiz.Tests.csproj: A test project
    - That tests the topology defined in the app

This can be built and ran via Docker, the docker file is included

You can also find a docker compose file (compose/docker-compose.yml) that sets up everything for the app to run. It has the following services

    - streamiz (localhost:8060) the app itself
    - kafka-ui (localhost:8080) to interact with the kafka topics
    - prometheus (localhost:9090) to pull metrics from otel-collector and visualize them
    - otel-collector, OpenTelemetry Collector where the app sends metrics
    - schema-registry
    - kafka-broker
    - zookeeper

to begin go to /compose and run `docker-compose up --build`