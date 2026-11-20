package com.pedidos360.kafka.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Registra {@link KafkaProperties}. La serialización String y el
 * {@code KafkaTemplate} los configura Spring Boot desde {@code spring.kafka.*}.
 */
@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaConfig {
}
