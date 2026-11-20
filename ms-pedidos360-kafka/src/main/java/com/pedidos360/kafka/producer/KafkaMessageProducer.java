package com.pedidos360.kafka.producer;

import com.pedidos360.kafka.config.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Productor del topic de la actividad. La key es opcional: cuando se envía,
 * Kafka la usa para elegir la partición (hash murmur2), igual que en la guía.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaMessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaProperties properties;

    public void send(String key, String message) {
        kafkaTemplate.send(properties.topic(), key, message);
        log.debug("Mensaje enviado al topic {}: key={} message={}", properties.topic(), key, message);
    }
}
