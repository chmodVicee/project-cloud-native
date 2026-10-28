package com.pedidos360.orders.messaging.support;

import com.pedidos360.orders.config.MessagingProperties;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Decide qué pasa con un mensaje que falló en un consumidor con ACK manual:
 * <ul>
 *   <li>Error no recuperable → {@link #reject}: NACK sin requeue → DLX → DLQ.</li>
 *   <li>Error recuperable con intentos disponibles → {@link #retryOrReject}: se copia a la
 *   cola de espera (TTL) con el contador incrementado y se hace ACK del original.</li>
 *   <li>Error recuperable sin intentos disponibles → NACK sin requeue → DLQ.</li>
 * </ul>
 * Nunca se hace NACK con requeue, para evitar bucles infinitos de reproceso.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MessageRetryHandler {

    /** Cabecera con la cantidad de reintentos ya realizados. */
    public static final String RETRY_COUNT_HEADER = "x-retry-count";

    private final RabbitTemplate rabbitTemplate;
    private final MessagingProperties properties;

    public void retryOrReject(Message message, Channel channel, String queueName, Exception cause) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        int retries = retryCount(message);
        int maxAttempts = properties.retry().maxAttempts();

        if (retries + 1 >= maxAttempts) {
            log.error("Mensaje {} de {} agotó {} intentos; se envía a la DLQ. Causa: {}",
                    messageId(message), queueName, maxAttempts, cause.getMessage());
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        message.getMessageProperties().setHeader(RETRY_COUNT_HEADER, retries + 1);
        try {
            rabbitTemplate.send(properties.exchanges().retry(), queueName, message);
        } catch (AmqpException ex) {
            log.error("No se pudo programar el reintento del mensaje {}; se envía a la DLQ",
                    messageId(message), ex);
            channel.basicNack(deliveryTag, false, false);
            return;
        }
        channel.basicAck(deliveryTag, false);
        log.warn("Mensaje {} de {} falló (intento {}/{}); se reintenta en {} ms. Causa: {}",
                messageId(message), queueName, retries + 1, maxAttempts,
                properties.retry().delayMs(), cause.getMessage());
    }

    public void reject(Message message, Channel channel, String queueName, Exception cause) throws IOException {
        log.error("Mensaje {} de {} rechazado sin reintento; se envía a la DLQ. Causa: {}",
                messageId(message), queueName, cause.getMessage());
        channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
    }

    private static int retryCount(Message message) {
        Object value = message.getMessageProperties().getHeader(RETRY_COUNT_HEADER);
        return value instanceof Number number ? number.intValue() : 0;
    }

    private static String messageId(Message message) {
        String id = message.getMessageProperties().getMessageId();
        return id != null ? id : "(sin id)";
    }
}
