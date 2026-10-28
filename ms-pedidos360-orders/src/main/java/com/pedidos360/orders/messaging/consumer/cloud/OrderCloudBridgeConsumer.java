package com.pedidos360.orders.messaging.consumer.cloud;

import com.pedidos360.orders.config.MessagingProperties;
import com.pedidos360.orders.event.OrderStatusEvent;
import com.pedidos360.orders.messaging.producer.cloud.CloudStatusPublisher;
import com.pedidos360.orders.messaging.support.MessageRetryHandler;
import com.pedidos360.orders.messaging.support.NonRecoverableMessageException;
import com.pedidos360.orders.messaging.support.OrderEventReader;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Consumidor del dominio "nube": reenvía cada evento de estado desde RabbitMQ hacia
 * GCP Pub/Sub. ACK manual:
 * <ul>
 *   <li>Pub/Sub confirmó la publicación → ACK.</li>
 *   <li>JSON inválido o incompleto → NACK sin requeue → DLQ.</li>
 *   <li>Pub/Sub no responde u otro error transitorio → reintento con espera; al agotar
 *   los intentos → DLQ.</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class OrderCloudBridgeConsumer {

    private final OrderEventReader eventReader;
    private final CloudStatusPublisher cloudStatusPublisher;
    private final MessageRetryHandler retryHandler;
    private final MessagingProperties properties;

    @RabbitListener(id = "orders-cloud-bridge", queues = "${messaging.rabbitmq.queues.cloud-bridge.name}")
    public void onOrderEvent(Message message, Channel channel) throws IOException {
        String queue = properties.queues().cloudBridge().name();
        try {
            OrderStatusEvent event = eventReader.read(message);
            cloudStatusPublisher.publish(event, message.getBody());
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        } catch (NonRecoverableMessageException ex) {
            retryHandler.reject(message, channel, queue, ex);
        } catch (RuntimeException ex) {
            retryHandler.retryOrReject(message, channel, queue, ex);
        }
    }
}
