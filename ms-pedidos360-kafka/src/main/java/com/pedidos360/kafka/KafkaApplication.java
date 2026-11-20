package com.pedidos360.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Microservicio de la actividad Kafka 3.2.1: productor REST, consumidor y
 * scripts de carga. Es independiente de la mensajería RabbitMQ/Pub-Sub de orders.
 */
@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }
}
