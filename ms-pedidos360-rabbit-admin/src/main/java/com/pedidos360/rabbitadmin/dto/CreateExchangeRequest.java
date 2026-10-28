package com.pedidos360.rabbitadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

/**
 * Datos para crear un exchange.
 *
 * @param type    DIRECT, TOPIC, FANOUT o HEADERS
 * @param durable sobrevive a reinicios del broker (por defecto true)
 */
public record CreateExchangeRequest(
        @NotBlank(message = "es obligatorio")
        @Pattern(regexp = RabbitNames.NAME_PATTERN, message = RabbitNames.NAME_MESSAGE)
        String name,
        @NotNull(message = "es obligatorio (DIRECT, TOPIC, FANOUT o HEADERS)")
        ExchangeType type,
        Boolean durable
) {

    public boolean isDurable() {
        return durable == null || durable;
    }
}
