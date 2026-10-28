package com.pedidos360.rabbitadmin.config;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;

/**
 * Reglas del administrador (prefijo {@code rabbit-admin} en application.yaml).
 *
 * @param protectedPrefixes prefijos de nombres que no se pueden crear ni eliminar por la API
 */
@Validated
@ConfigurationProperties(prefix = "rabbit-admin")
public record RabbitAdminProperties(@NotNull List<String> protectedPrefixes) {

    public boolean isProtected(String name) {
        return protectedPrefixes.stream().anyMatch(name::startsWith);
    }
}
