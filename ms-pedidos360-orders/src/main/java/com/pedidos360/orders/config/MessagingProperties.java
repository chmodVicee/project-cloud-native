package com.pedidos360.orders.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
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
        @Valid @NotNull RoutingKeys routingKeys,
        @Valid @NotNull Queues queues,
        @Valid @NotNull Retry retry
) {

    /**
     * @param orders     exchange topic donde se publican los eventos de estado de los pedidos
     * @param retry      exchange direct que deja los mensajes en las colas de espera de reintento
     * @param deadLetter exchange direct (DLX) que recibe los mensajes rechazados y los lleva a las DLQ
     */
    public record Exchanges(@NotBlank String orders,
                            @NotBlank String retry,
                            @NotBlank String deadLetter) {
    }

    /**
     * @param orderCreated       routing key del evento "pedido creado" (estado inicial)
     * @param orderStatusChanged routing key del evento "cambio de estado"
     * @param allOrderEvents     patrón topic con el que las colas se suscriben a todos los eventos
     */
    public record RoutingKeys(@NotBlank String orderCreated,
                              @NotBlank String orderStatusChanged,
                              @NotBlank String allOrderEvents) {
    }

    /**
     * @param notifications ruta del caso de uso "notificar al cliente"
     * @param cloudBridge   ruta del caso de uso "reenviar el estado a GCP Pub/Sub"
     */
    public record Queues(@Valid @NotNull QueueRoute notifications,
                         @Valid @NotNull QueueRoute cloudBridge) {
    }

    /**
     * Colas de un caso de uso: la principal, su cola de espera para reintentos y su DLQ.
     * El nombre de la cola principal es también la routing key en el exchange de reintentos,
     * y el de la DLQ la routing key en el DLX.
     */
    public record QueueRoute(@NotBlank String name,
                             @NotBlank String retryQueue,
                             @NotBlank String deadLetterQueue) {
    }

    /**
     * @param maxAttempts intentos totales (incluido el primero) antes de enviar a la DLQ
     * @param delayMs     espera en la cola de reintento antes de volver a la cola principal
     */
    public record Retry(@Min(1) int maxAttempts,
                        @Min(100) long delayMs) {
    }
}
