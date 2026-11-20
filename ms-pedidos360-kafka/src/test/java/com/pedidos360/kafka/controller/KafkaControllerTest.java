package com.pedidos360.kafka.controller;

import com.pedidos360.kafka.producer.KafkaMessageProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class KafkaControllerTest {

    private KafkaMessageProducer producer;
    private KafkaController controller;

    @BeforeEach
    void setUp() {
        producer = mock(KafkaMessageProducer.class);
        controller = new KafkaController(producer);
    }

    @Test
    void enviaMensajeConKey() {
        assertThat(controller.sendMessage("hola", "orden-1"))
                .isEqualTo("Mensaje enviado: hola | Key: orden-1");

        verify(producer).send("orden-1", "hola");
        verifyNoMoreInteractions(producer);
    }

    @Test
    void enviaMensajeSinKey() {
        assertThat(controller.sendMessage("hola", null))
                .isEqualTo("Mensaje enviado: hola | Key: null");

        verify(producer).send(null, "hola");
        verifyNoMoreInteractions(producer);
    }
}
