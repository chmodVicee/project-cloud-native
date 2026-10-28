package com.pedidos360.orders.messaging;

import com.pedidos360.orders.config.MessagingProperties;

/** Propiedades de mensajería con nombres de prueba para los tests unitarios. */
public final class TestMessagingProperties {

    private TestMessagingProperties() {
    }

    public static MessagingProperties create() {
        return new MessagingProperties(
                new MessagingProperties.Exchanges("test.orders", "test.retry", "test.dlx"),
                new MessagingProperties.RoutingKeys("order.created", "order.status.changed", "order.#"),
                new MessagingProperties.Queues(
                        new MessagingProperties.QueueRoute("test.notifications", "test.notifications.retry",
                                "test.notifications.dlq"),
                        new MessagingProperties.QueueRoute("test.cloud", "test.cloud.retry", "test.cloud.dlq")),
                new MessagingProperties.Retry(3, 1000));
    }
}
