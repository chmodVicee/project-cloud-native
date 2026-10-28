package com.pedidos360.rabbitadmin.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuración del administrador. La conexión al broker ({@code spring.rabbitmq.*}) y los
 * beans {@code AmqpAdmin}/{@code RabbitTemplate} los crea Spring Boot; aquí solo se
 * registran las reglas propias del administrador.
 */
@Configuration
@EnableConfigurationProperties(RabbitAdminProperties.class)
public class RabbitAdminConfig {
}
