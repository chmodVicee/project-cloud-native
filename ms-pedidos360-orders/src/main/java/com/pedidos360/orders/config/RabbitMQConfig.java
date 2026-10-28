package com.pedidos360.orders.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

/**
 * Configuración de RabbitMQ del microservicio de órdenes.
 *
 * <p>Rutas de mensajería declaradas:
 * <ul>
 *   <li><b>Eventos de estado</b>: exchange topic {@code exchanges.orders}; el productor
 *   publica con {@code routing-keys.order-created} y {@code routing-keys.order-status-changed}.</li>
 * </ul>
 * Los nombres vienen de {@link MessagingProperties} (application.yaml).
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(MessagingProperties.class)
public class RabbitMQConfig {

    /** Exchange principal de eventos de pedidos (topic, durable). */
    @Bean
    public TopicExchange ordersExchange(MessagingProperties properties) {
        return ExchangeBuilder.topicExchange(properties.exchanges().orders())
                .durable(true)
                .build();
    }

    /** Serializa los eventos como JSON usando el JsonMapper de Spring Boot. */
    @Bean
    public MessageConverter messageConverter(JsonMapper jsonMapper) {
        return new JacksonJsonMessageConverter(jsonMapper);
    }

    /**
     * RabbitTemplate con confirmaciones del broker y retorno de mensajes no enrutables,
     * para que ningún mensaje no entregado pase sin quedar registrado en los logs.
     */
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        template.setMandatory(true);
        template.setConfirmCallback((correlation, ack, cause) -> {
            if (!ack) {
                log.error("RabbitMQ rechazó el mensaje {}: {}",
                        correlation != null ? correlation.getId() : "-", cause);
            }
        });
        template.setReturnsCallback(returned -> log.warn(
                "Mensaje no enrutable devuelto por RabbitMQ: exchange={}, routingKey={}, motivo={}",
                returned.getExchange(), returned.getRoutingKey(), returned.getReplyText()));
        return template;
    }
}
