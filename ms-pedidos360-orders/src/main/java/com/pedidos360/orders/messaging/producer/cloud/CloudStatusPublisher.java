package com.pedidos360.orders.messaging.producer.cloud;

import com.pedidos360.orders.event.OrderStatusEvent;
import com.pedidos360.orders.messaging.support.RecoverableMessageException;

/**
 * Publica el estado de un pedido en la cola de mensajes de la nube (GCP Pub/Sub).
 */
public interface CloudStatusPublisher {

    /**
     * @param event   evento ya validado
     * @param payload JSON original del mensaje, que se reenvía sin cambios
     * @throws RecoverableMessageException si la nube no confirma la publicación
     */
    void publish(OrderStatusEvent event, byte[] payload);
}
