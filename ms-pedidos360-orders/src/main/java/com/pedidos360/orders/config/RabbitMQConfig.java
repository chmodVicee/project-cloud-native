package com.pedidos360.orders.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.boot.amqp.autoconfigure.SimpleRabbitListenerContainerFactoryConfigurer;
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
 *   <li><b>Notificaciones</b>: {@code exchanges.orders} --(order.#)--> {@code queues.notifications.name}.</li>
 *   <li><b>Reintentos</b>: {@code exchanges.retry} --(nombre de la cola)--> cola de espera con TTL
 *   {@code retry.delay-ms}; al expirar vuelve a la cola principal por el exchange por defecto.</li>
 *   <li><b>Mensajes muertos</b>: la cola principal rechaza (NACK sin requeue) hacia el DLX
 *   {@code exchanges.dead-letter}, que los deja en la DLQ del caso de uso.</li>
 * </ul>
 * Los nombres vienen de {@link MessagingProperties} (application.yaml).
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(MessagingProperties.class)
public class RabbitMQConfig {

    // ---------- Exchanges ----------

    /** Exchange principal de eventos de pedidos (topic, durable). */
    @Bean
    public TopicExchange ordersExchange(MessagingProperties properties) {
        return ExchangeBuilder.topicExchange(properties.exchanges().orders())
                .durable(true)
                .build();
    }

    /** Exchange que lleva los mensajes con error recuperable a su cola de espera. */
    @Bean
    public DirectExchange retryExchange(MessagingProperties properties) {
        return ExchangeBuilder.directExchange(properties.exchanges().retry())
                .durable(true)
                .build();
    }

    /** Dead Letter Exchange: recibe los mensajes rechazados y los enruta a las DLQ. */
    @Bean
    public DirectExchange deadLetterExchange(MessagingProperties properties) {
        return ExchangeBuilder.directExchange(properties.exchanges().deadLetter())
                .durable(true)
                .build();
    }

    // ---------- Caso de uso: notificaciones al cliente ----------

    @Bean
    public Queue notificationsQueue(MessagingProperties properties) {
        return mainQueue(properties, properties.queues().notifications());
    }

    @Bean
    public Queue notificationsRetryQueue(MessagingProperties properties) {
        return retryQueue(properties, properties.queues().notifications());
    }

    @Bean
    public Queue notificationsDeadLetterQueue(MessagingProperties properties) {
        return QueueBuilder.durable(properties.queues().notifications().deadLetterQueue()).build();
    }

    /** Todos los eventos de pedidos (order.#) llegan a la cola de notificaciones. */
    @Bean
    public Binding notificationsBinding(Queue notificationsQueue, TopicExchange ordersExchange,
                                        MessagingProperties properties) {
        return BindingBuilder.bind(notificationsQueue).to(ordersExchange)
                .with(properties.routingKeys().allOrderEvents());
    }

    @Bean
    public Binding notificationsRetryBinding(Queue notificationsRetryQueue, DirectExchange retryExchange,
                                             MessagingProperties properties) {
        return BindingBuilder.bind(notificationsRetryQueue).to(retryExchange)
                .with(properties.queues().notifications().name());
    }

    @Bean
    public Binding notificationsDeadLetterBinding(Queue notificationsDeadLetterQueue, DirectExchange deadLetterExchange,
                                                  MessagingProperties properties) {
        return BindingBuilder.bind(notificationsDeadLetterQueue).to(deadLetterExchange)
                .with(properties.queues().notifications().deadLetterQueue());
    }

    // ---------- Conversión y envío ----------

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

    /**
     * Fábrica de los {@code @RabbitListener}: toma la configuración de spring.rabbitmq.listener
     * (ACK manual, prefetch) pero entrega el mensaje sin convertir. Así un JSON inválido llega
     * al consumidor, que decide explícitamente el NACK hacia la DLQ, en vez de fallar antes
     * dentro del contenedor.
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer, ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setMessageConverter(new SimpleMessageConverter());
        return factory;
    }

    // ---------- Plantillas de colas ----------

    /** Cola principal durable cuyos mensajes rechazados van al DLX con destino a su DLQ. */
    private static Queue mainQueue(MessagingProperties properties, MessagingProperties.QueueRoute route) {
        return QueueBuilder.durable(route.name())
                .deadLetterExchange(properties.exchanges().deadLetter())
                .deadLetterRoutingKey(route.deadLetterQueue())
                .build();
    }

    /**
     * Cola de espera sin consumidores: el mensaje expira tras {@code retry.delay-ms} y el
     * exchange por defecto ("") lo devuelve a la cola principal (routing key = su nombre).
     */
    private static Queue retryQueue(MessagingProperties properties, MessagingProperties.QueueRoute route) {
        return QueueBuilder.durable(route.retryQueue())
                .ttl((int) properties.retry().delayMs())
                .deadLetterExchange("")
                .deadLetterRoutingKey(route.name())
                .build();
    }
}
