package com.pedidos360.orders.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Nombres centralizados de la mensajería RabbitMQ (prefijo {@code messaging.rabbitmq}
 * en application.yaml). Ninguna clase de negocio ni consumidor escribe nombres de
 * exchanges, colas o routing keys a mano: todos salen de aquí.
 */
@Validated
@ConfigurationProperties(prefix = "messaging.rabbitmq")
public record MessagingProperties(
        @Valid @NotNull Exchanges exchanges,
        @Valid @NotNull RoutingKeys routingKeys
) {

    /**
     * @param orders exchange topic donde se publican los eventos de estado de los pedidos
     */
    public record Exchanges(@NotBlank String orders) {
    }

    /**
     * @param orderCreated       routing key del evento "pedido creado" (estado inicial)
     * @param orderStatusChanged routing key del evento "cambio de estado"
     */
    public record RoutingKeys(@NotBlank String orderCreated,
                              @NotBlank String orderStatusChanged) {
    }
}
