package com.pedidos360.orders.messaging.support;

import com.pedidos360.orders.event.OrderStatusEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Convierte el cuerpo JSON de un mensaje en {@link OrderStatusEvent}. Un JSON inválido
 * o sin los campos obligatorios es un error no recuperable.
 */
@Component
@RequiredArgsConstructor
public class OrderEventReader {

    private final JsonMapper jsonMapper;

    public OrderStatusEvent read(Message message) {
        OrderStatusEvent event;
        try {
            event = jsonMapper.readValue(message.getBody(), OrderStatusEvent.class);
        } catch (JacksonException ex) {
            throw new NonRecoverableMessageException("JSON inválido: " + ex.getOriginalMessage(), ex);
        }
        if (event == null || event.orderId() == null || event.newStatus() == null
                || event.userEmail() == null || event.userEmail().isBlank()) {
            throw new NonRecoverableMessageException("El evento debe incluir orderId, userEmail y newStatus");
        }
        return event;
    }
}
