package com.pedidos360.orders.messaging.producer;

import com.pedidos360.orders.config.MessagingProperties;
import com.pedidos360.orders.event.OrderStatusEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * Productor de eventos de estado de pedidos hacia RabbitMQ.
 *
 * <p>Escucha el {@link OrderStatusEvent} que publica {@code OrderService} y lo envía
 * <b>después del commit</b>: si la transacción falla no sale ningún mensaje.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    private final RabbitTemplate rabbitTemplate;
    private final MessagingProperties properties;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void publish(OrderStatusEvent event) {
        String exchange = properties.exchanges().orders();
        String routingKey = event.isCreation()
                ? properties.routingKeys().orderCreated()
                : properties.routingKeys().orderStatusChanged();
        try {
            rabbitTemplate.convertAndSend(exchange, routingKey, event, message -> {
                message.getMessageProperties().setMessageId(event.eventId());
                return message;
            }, new CorrelationData(event.eventId()));
            log.info("Evento publicado [{}] pedido #{}: {} -> {}",
                    routingKey, event.orderId(), event.previousStatus(), event.newStatus());
        } catch (AmqpException ex) {
            // El estado ya quedó guardado; se registra el mensaje que no se pudo entregar.
            log.error("No se pudo publicar el evento {} del pedido #{} en {}/{}",
                    event.eventId(), event.orderId(), exchange, routingKey, ex);
        }
    }
}
