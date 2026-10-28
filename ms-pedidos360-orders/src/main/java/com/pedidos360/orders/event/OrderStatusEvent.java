package com.pedidos360.orders.event;

import com.pedidos360.orders.model.Order;
import com.pedidos360.orders.model.OrderStatus;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;

/**
 * Evento de dominio: un pedido se creó o cambió de estado. Es también el contenido
 * (JSON) del mensaje que viaja por RabbitMQ y Pub/Sub.
 *
 * @param previousStatus estado anterior; {@code null} cuando el pedido recién se crea
 */
public record OrderStatusEvent(
        String eventId,
        Long orderId,
        String userEmail,
        OrderStatus previousStatus,
        OrderStatus newStatus,
        BigDecimal total,
        OffsetDateTime occurredAt
) {

    public static OrderStatusEvent of(Order order, OrderStatus previousStatus) {
        return new OrderStatusEvent(
                UUID.randomUUID().toString(),
                order.getId(),
                order.getUserEmail(),
                previousStatus,
                order.getStatus(),
                order.getTotal(),
                OffsetDateTime.now(ZoneId.of("America/Santiago")));
    }

    public boolean isCreation() {
        return previousStatus == null;
    }
}
