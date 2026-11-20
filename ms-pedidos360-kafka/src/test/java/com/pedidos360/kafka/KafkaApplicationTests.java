package com.pedidos360.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/** Comprueba que el contexto del microservicio (productor, consumidor y endpoint) arranca. */
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class KafkaApplicationTests {

    @Test
    void contextLoads() {
    }
}
