package com.pedidos360.orders.messaging.consumer.notification;

import com.pedidos360.orders.config.MessagingProperties;
import com.pedidos360.orders.exception.ResourceNotFoundException;
import com.pedidos360.orders.messaging.support.MessageRetryHandler;
import com.pedidos360.orders.messaging.support.NonRecoverableMessageException;
import com.pedidos360.orders.messaging.support.OrderEventReader;
import com.pedidos360.orders.service.OrderNotificationService;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumidor del dominio "notificaciones": escucha todos los eventos de pedidos y avisa
 * al cliente. ACK manual:
 * <ul>
 *   <li>Éxito → ACK.</li>
 *   <li>JSON inválido o pedido inexistente → NACK sin requeue → DLQ.</li>
 *   <li>Cualquier otro error (p. ej. base de datos caída) → reintento con espera; al
 *   agotar los intentos → DLQ.</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class OrderNotificationConsumer {

    private final OrderEventReader eventReader;
    private final OrderNotificationService notificationService;
    private final MessageRetryHandler retryHandler;
    private final MessagingProperties properties;

    @RabbitListener(id = "orders-notifications", queues = "${messaging.rabbitmq.queues.notifications.name}")
    public void onOrderEvent(Message message, Channel channel) throws IOException {
        String queue = properties.queues().notifications().name();
        try {
            notificationService.notifyCustomer(eventReader.read(message));
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        } catch (NonRecoverableMessageException | ResourceNotFoundException ex) {
            retryHandler.reject(message, channel, queue, ex);
        } catch (RuntimeException ex) {
            retryHandler.retryOrReject(message, channel, queue, ex);
        }
    }
}
