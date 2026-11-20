package com.pedidos360.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Consumidor del topic de la actividad: muestra la partición, el offset,
 * la key y el mensaje recibido para poder comprobar el reparto por particiones.
 */
@Slf4j
@Service
public class KafkaMessageConsumer {

    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group-id}")
    public void listen(@Payload String message,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) long offset,
                       @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key) {
        log.info("MENSAJE RECIBIDO | particion={} | offset={} | key={} | mensaje={}",
                partition, offset, key != null ? key : "(sin key)", message);
    }
}
