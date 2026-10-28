package com.pedidos360.orders.messaging.consumer.deadletter;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Consumidor de las DLQ: registra en el log cada mensaje no entregado (cola de origen,
 * motivo y contenido) y hace ACK para sacarlo de la DLQ una vez auditado.
 */
@Slf4j
@Component
public class DeadLetterConsumer {

    private static final int MAX_BODY_LOG = 500;

    @RabbitListener(id = "orders-dead-letters",
            queues = "${messaging.rabbitmq.queues.notifications.dead-letter-queue}")
    public void onDeadLetter(Message message, Channel channel) throws IOException {
        MessageProperties props = message.getMessageProperties();
        List<Map<String, ?>> xDeath = props.getXDeathHeader();
        Map<String, ?> death = xDeath != null && !xDeath.isEmpty() ? xDeath.get(0) : Map.of();

        log.error("[DLQ {}] mensaje {} no entregado. Cola de origen={}, motivo={}, contenido={}",
                props.getConsumerQueue(),
                props.getMessageId() != null ? props.getMessageId() : "(sin id)",
                death.containsKey("queue") ? death.get("queue") : "-",
                death.containsKey("reason") ? death.get("reason") : "-",
                abbreviate(new String(message.getBody(), StandardCharsets.UTF_8)));
        channel.basicAck(props.getDeliveryTag(), false);
    }

    private static String abbreviate(String body) {
        return body.length() <= MAX_BODY_LOG ? body : body.substring(0, MAX_BODY_LOG) + "...";
    }
}
