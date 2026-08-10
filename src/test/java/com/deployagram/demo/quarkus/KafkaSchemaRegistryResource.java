package com.deployagram.demo.quarkus;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static io.restassured.RestAssured.given;

public class KafkaSchemaRegistryResource implements QuarkusTestResourceLifecycleManager {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.6.1");
    private static final DockerImageName SCHEMA_REGISTRY_IMAGE = DockerImageName.parse("confluentinc/cp-schema-registry:7.6.1");

    private static final String DEPLOYAGRAM_NETWORK = "deployagram";
    private static final int KAFKA_COLLECTOR_PORT = 19092;

    private final String containerNameSuffix = UUID.randomUUID().toString().substring(0, 8);
    private final String kafkaContainerName = "bookoverflow-avro-kafka-" + containerNameSuffix;
    private final String schemaRegistryContainerName = "bookoverflow-avro-schema-registry-" + containerNameSuffix;

    private KafkaContainer kafka;
    private GenericContainer<?> schemaRegistry;

    @Override
    public Map<String, String> start() {
        kafka = new KafkaContainer(KAFKA_IMAGE)
                .withNetworkMode(DEPLOYAGRAM_NETWORK)
                .withCreateContainerCmdModifier(command -> command.withName(kafkaContainerName))
                .withListener(() -> kafkaContainerName + ":" + KAFKA_COLLECTOR_PORT);
        kafka.start();
        schemaRegistry = new GenericContainer<>(SCHEMA_REGISTRY_IMAGE)
                .withNetworkMode(DEPLOYAGRAM_NETWORK)
                .withCreateContainerCmdModifier(command -> command.withName(schemaRegistryContainerName))
                .withExposedPorts(8081)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaBootstrapServersForCollector())
                .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofSeconds(30)));

        schemaRegistry.start();

        String schemaRegistryUrl = "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);

        return Map.of(
                "kafka.bootstrap.servers", kafka.getBootstrapServers(),
                "mp.messaging.connector.smallrye-kafka.bootstrap.servers", kafka.getBootstrapServers(),
                "mp.messaging.connector.smallrye-kafka.schema.registry.url", schemaRegistryUrl
        );
    }

    private String kafkaBootstrapServersForCollector() {
        return "PLAINTEXT://" + kafkaContainerName + ":" + KAFKA_COLLECTOR_PORT;
    }

    @Override
    public void stop() {
        if (schemaRegistry != null) {
            schemaRegistry.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
    }
}
