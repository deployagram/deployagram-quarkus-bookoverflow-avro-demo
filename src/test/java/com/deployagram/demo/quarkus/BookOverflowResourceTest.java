package com.deployagram.demo.quarkus;

import com.deployagram.demo.quarkus.avro.BookSuggestionMessage;
import com.deployagram.demo.quarkus.avro.DontForgetMessage;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static io.restassured.RestAssured.given;
import static net.javacrumbs.jsonunit.fluent.JsonFluentAssert.assertThatJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@QuarkusTest
@QuarkusTestResource(KafkaSchemaRegistryResource.class)
@ConnectWireMock
class BookOverflowResourceTest {

    protected static final int QUARKUS_APP_PORT = 8081;
    protected static final int WIREMOCK_PORT = 9050;

    private static final String EMAIL_ADDRESS = "test@deployagram.com";
    private static final String SUGGESTIONS_TOPIC = "SuppliedSuggestionsTopic";

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @ConfigProperty(name = "mp.messaging.connector.smallrye-kafka.schema.registry.url")
    String schemaRegistryUrl;

    WireMock wiremock;

    @Test
    void callCForBook() {
        String expectedRecommendation = """
                {
                  "name": "Refactoring",
                  "edition": "1st",
                  "format": "paperback",
                  "authors": [
                    "Fowler Martin",
                    "Beck Kent"
                  ]
                }
                """;
        wiremock.register(get(urlEqualTo("/ClassicBooks/random"))
                .withHeader("Accept", equalTo("application/json"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(expectedRecommendation)));

        String body = given()
                .port(QUARKUS_APP_PORT)
                .queryParam("email", EMAIL_ADDRESS)
                .when().get("/BookOverflow/suggestion")
                .then()
                .statusCode(200)
                .assertThat()
                .contentType(ContentType.JSON)
                .extract().body().asString();

        assertThatJson(body).isEqualTo(expectedRecommendation);
        assertExpectedKafkaMessage();
    }

    private void assertExpectedKafkaMessage() {
        List<ConsumerRecord<String, DontForgetMessage>> receivedRecords = new ArrayList<>();

        try (KafkaConsumer<String, DontForgetMessage> consumer = new KafkaConsumer<>(consumerProperties())) {
            consumer.subscribe(List.of(SUGGESTIONS_TOPIC));

            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(100))
                    .untilAsserted(() -> {
                        consumer.poll(Duration.ofMillis(500)).forEach(receivedRecords::add);
                        assertEquals(1, receivedRecords.size());

                        ConsumerRecord<String, DontForgetMessage> record = receivedRecords.get(0);
                        assertNotNull(record.value());

                        DontForgetMessage dontForget = record.value();
                        assertEquals(EMAIL_ADDRESS, dontForget.getEmail().toString());
                        BookSuggestionMessage suggestion = dontForget.getSuggestion();
                        assertEquals("Refactoring", suggestion.getName().toString());
                        assertEquals("1st", suggestion.getEdition().toString());
                        assertEquals("paperback", suggestion.getFormat().toString());
                    });
        }
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bookoverflow-avro-test-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        return properties;
    }
}
