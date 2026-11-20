package com.pedidos360.kafka;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.pedidos360.kafka.consumer.KafkaMessageConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Recorrido completo: endpoint/productor -> broker -> topic -> @KafkaListener. */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafka.topic=tutorial-particiones",
        "kafka.group-id=mi-grupo-educativo-test"
})
@EmbeddedKafka(partitions = 3, topics = "tutorial-particiones")
class KafkaIntegrationTest {

    private static final String TOPIC = "tutorial-particiones";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private final Logger logger = (Logger) LoggerFactory.getLogger(KafkaMessageConsumer.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    @BeforeEach
    void setUp() {
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(appender);
        appender.stop();
    }

    @Test
    void elMensajePublicadoLlegaAlTopicYEsConsumido() throws Exception {
        kafkaTemplate.send(TOPIC, "orden-1", "mensaje-integracion").get(10, TimeUnit.SECONDS);

        Map<String, Object> props = KafkaTestUtils.consumerProps("verificacion-test", "true", embeddedKafkaBroker);
        try (Consumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(
                props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(java.util.List.of(TOPIC));
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, TOPIC, Duration.ofSeconds(15));

            assertThat(record.key()).isEqualTo("orden-1");
            assertThat(record.value()).isEqualTo("mensaje-integracion");
        }

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(appender.list)
                        .extracting(ILoggingEvent::getFormattedMessage)
                        .anyMatch(log -> log.contains("mensaje=mensaje-integracion")
                                && log.contains("key=orden-1")));
    }
}
