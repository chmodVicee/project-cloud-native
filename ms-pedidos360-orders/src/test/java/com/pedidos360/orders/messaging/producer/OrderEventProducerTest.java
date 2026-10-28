package com.pedidos360.orders.messaging.producer;

import com.pedidos360.orders.event.OrderStatusEvent;
import com.pedidos360.orders.messaging.TestMessagingProperties;
import com.pedidos360.orders.model.OrderStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.math.BigDecimal;
import java.net.ConnectException;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrderEventProducerTest {

    private RabbitTemplate rabbitTemplate;
    private OrderEventProducer producer;

    @BeforeEach
    void setUp() {
        rabbitTemplate = mock(RabbitTemplate.class);
        producer = new OrderEventProducer(rabbitTemplate, TestMessagingProperties.create());
    }

    @Test
    void publicaPedidoCreadoConRoutingKeyDeCreacion() {
        OrderStatusEvent event = event(null, OrderStatus.RECIBIDO);

        producer.publish(event);

        verify(rabbitTemplate).convertAndSend(eq("test.orders"), eq("order.created"), eq(event),
                any(MessagePostProcessor.class), any(CorrelationData.class));
    }

    @Test
    void publicaCambioDeEstadoConRoutingKeyDeCambio() {
        OrderStatusEvent event = event(OrderStatus.RECIBIDO, OrderStatus.CONFIRMADO);

        producer.publish(event);

        verify(rabbitTemplate).convertAndSend(eq("test.orders"), eq("order.status.changed"), eq(event),
                any(MessagePostProcessor.class), any(CorrelationData.class));
    }

    @Test
    void siElBrokerNoEstaDisponibleNoPropagaLaExcepcion() {
        doThrow(new AmqpConnectException(new ConnectException("broker caído")))
                .when(rabbitTemplate).convertAndSend(any(String.class), any(String.class), any(Object.class),
                        any(MessagePostProcessor.class), any(CorrelationData.class));

        assertThatCode(() -> producer.publish(event(OrderStatus.LISTO, OrderStatus.ENTREGADO)))
                .doesNotThrowAnyException();
    }

    private OrderStatusEvent event(OrderStatus previous, OrderStatus next) {
        return new OrderStatusEvent("evt-1", 1L, "cliente@test.cl", previous, next,
                BigDecimal.TEN, OffsetDateTime.now());
    }
}
