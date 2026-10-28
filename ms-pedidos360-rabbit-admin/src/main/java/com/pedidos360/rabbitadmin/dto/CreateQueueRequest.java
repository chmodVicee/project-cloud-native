package com.pedidos360.rabbitadmin.dto;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

/**
 * Datos para crear una cola.
 *
 * @param durable              sobrevive a reinicios del broker (por defecto true)
 * @param autoDelete           se elimina cuando se va su último consumidor (por defecto false)
 * @param deadLetterExchange   exchange al que van los mensajes rechazados o expirados (DLX)
 * @param deadLetterRoutingKey routing key con la que se reenvían al DLX
 * @param messageTtlMs         tiempo de vida de cada mensaje en milisegundos
 * @param maxLength            cantidad máxima de mensajes en la cola
 */
public record CreateQueueRequest(
        @NotBlank(message = "es obligatorio")
        @Pattern(regexp = RabbitNames.NAME_PATTERN, message = RabbitNames.NAME_MESSAGE)
        String name,
        Boolean durable,
        Boolean autoDelete,
        @Pattern(regexp = RabbitNames.NAME_PATTERN, message = RabbitNames.NAME_MESSAGE)
        String deadLetterExchange,
        @Pattern(regexp = RabbitNames.ROUTING_KEY_PATTERN, message = RabbitNames.ROUTING_KEY_MESSAGE)
        String deadLetterRoutingKey,
        @Positive(message = "debe ser mayor que 0")
        @Max(value = Integer.MAX_VALUE, message = "es demasiado grande")
        Long messageTtlMs,
        @Positive(message = "debe ser mayor que 0")
        Integer maxLength
) {

    public boolean isDurable() {
        return durable == null || durable;
    }

    public boolean isAutoDelete() {
        return autoDelete != null && autoDelete;
    }

    @AssertTrue(message = "deadLetterRoutingKey requiere deadLetterExchange")
    public boolean isDeadLetterConfigValid() {
        return deadLetterRoutingKey == null || deadLetterExchange != null;
    }
}
