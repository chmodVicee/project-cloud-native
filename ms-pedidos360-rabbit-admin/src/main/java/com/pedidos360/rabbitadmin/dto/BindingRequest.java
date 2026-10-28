package com.pedidos360.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

/**
 * Datos de un binding: une {@code exchange} con {@code destination} (cola u otro exchange)
 * mediante {@code routingKey}. Se usa como cuerpo al crear y como query params al eliminar.
 *
 * @param routingKey puede ser vacía (por ejemplo en exchanges fanout); null equivale a vacía
 */
public record BindingRequest(
        @NotBlank(message = "es obligatorio")
        @Pattern(regexp = RabbitNames.NAME_PATTERN, message = RabbitNames.NAME_MESSAGE)
        String exchange,
        @NotBlank(message = "es obligatorio")
        @Pattern(regexp = RabbitNames.NAME_PATTERN, message = RabbitNames.NAME_MESSAGE)
        String destination,
        @NotNull(message = "es obligatorio (QUEUE o EXCHANGE)")
        DestinationType destinationType,
        @Pattern(regexp = RabbitNames.ROUTING_KEY_PATTERN, message = RabbitNames.ROUTING_KEY_MESSAGE)
        String routingKey
) {

    public String routingKeyOrEmpty() {
        return routingKey == null ? "" : routingKey;
    }
}
