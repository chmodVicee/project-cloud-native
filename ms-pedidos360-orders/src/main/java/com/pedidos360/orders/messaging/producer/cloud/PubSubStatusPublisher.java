package com.pedidos360.orders.messaging.producer.cloud;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.pedidos360.orders.config.PubSubProperties;
import com.pedidos360.orders.event.OrderStatusEvent;
import com.pedidos360.orders.messaging.support.RecoverableMessageException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publica en el topic de Pub/Sub y espera la confirmación (message id). Si no llega a
 * tiempo, lanza {@link RecoverableMessageException} para que RabbitMQ reintente.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "messaging.pubsub", name = "enabled", havingValue = "true")
public class PubSubStatusPublisher implements CloudStatusPublisher {

    private final Publisher orderStatusPublisher;
    private final PubSubProperties properties;

    @Override
    public void publish(OrderStatusEvent event, byte[] payload) {
        PubsubMessage message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(payload))
                .putAttributes("orderId", String.valueOf(event.orderId()))
                .putAttributes("newStatus", event.newStatus().name())
                .putAttributes("eventId", String.valueOf(event.eventId()))
                .build();
        try {
            String messageId = orderStatusPublisher.publish(message)
                    .get(properties.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
            log.info("Estado del pedido #{} ({}) publicado en Pub/Sub {} con id {}",
                    event.orderId(), event.newStatus(), properties.topic(), messageId);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RecoverableMessageException("Publicación en Pub/Sub interrumpida", ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new RecoverableMessageException("Pub/Sub no confirmó la publicación: " + ex.getMessage(), ex);
        }
    }
}
