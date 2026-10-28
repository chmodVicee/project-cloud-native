package com.pedidos360.orders.messaging.producer.cloud;

import com.pedidos360.orders.event.OrderStatusEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Implementación usada con {@code messaging.pubsub.enabled=false}: no envía nada a la nube,
 * solo lo deja registrado, y el mensaje de RabbitMQ se confirma normalmente.
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "messaging.pubsub", name = "enabled", havingValue = "false", matchIfMissing = true)
public class DisabledCloudStatusPublisher implements CloudStatusPublisher {

    @Override
    public void publish(OrderStatusEvent event, byte[] payload) {
        log.info("Pub/Sub deshabilitado: el estado {} del pedido #{} no se reenvía a la nube",
                event.newStatus(), event.orderId());
    }
}
