package com.pedidos360.kafka.producer;

import com.pedidos360.kafka.config.KafkaProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class KafkaMessageProducerTest {

    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaMessageProducer producer;

    @BeforeEach
    void setUp() {
        kafkaTemplate = mock(KafkaTemplate.class);
        producer = new KafkaMessageProducer(kafkaTemplate, new KafkaProperties("test-topic", "test-group"));
    }

    @Test
    void publicaEnElTopicConfiguradoConKeyYMensaje() {
        producer.send("orden-1", "mensaje-1");

        verify(kafkaTemplate).send("test-topic", "orden-1", "mensaje-1");
    }

    @Test
    void permiteEnviarSinKey() {
        producer.send(null, "mensaje-sin-key");

        verify(kafkaTemplate).send("test-topic", null, "mensaje-sin-key");
    }
}
