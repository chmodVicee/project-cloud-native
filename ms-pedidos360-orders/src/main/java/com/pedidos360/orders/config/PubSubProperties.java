package com.pedidos360.orders.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Configuración de Google Cloud Pub/Sub (prefijo {@code messaging.pubsub} en application.yaml).
 *
 * @param enabled         activa el reenvío de estados a Pub/Sub y el subscriber
 * @param projectId       proyecto de GCP (en el emulador puede ser cualquier id)
 * @param emulatorHost    host:puerto del emulador; vacío = Pub/Sub real con credenciales ADC
 * @param topic           topic donde se publican los estados de pedidos
 * @param subscription    suscripción que consume el subscriber de auditoría
 * @param publishTimeout  tiempo máximo de espera de la confirmación de Pub/Sub
 * @param createResources crea el topic y la suscripción al arrancar si no existen
 */
@Validated
@ConfigurationProperties(prefix = "messaging.pubsub")
public record PubSubProperties(
        boolean enabled,
        @NotBlank String projectId,
        String emulatorHost,
        @NotBlank String topic,
        @NotBlank String subscription,
        @NotNull Duration publishTimeout,
        boolean createResources
) {

    public boolean usesEmulator() {
        return emulatorHost != null && !emulatorHost.isBlank();
    }
}
