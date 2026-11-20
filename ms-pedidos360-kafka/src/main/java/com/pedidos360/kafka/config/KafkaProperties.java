package com.pedidos360.kafka.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Nombre del topic y grupo de consumidores del ejemplo (prefijo {@code kafka}
 * en application.yaml), siguiendo el patrón de properties centralizadas de orders.
 */
@Validated
@ConfigurationProperties(prefix = "kafka")
public record KafkaProperties(@NotBlank String topic,
                              @NotBlank String groupId) {
}
