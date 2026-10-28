package com.pedidos360.orders.messaging.consumer.notification;

import com.pedidos360.orders.exception.ResourceNotFoundException;
import com.pedidos360.orders.messaging.TestMessagingProperties;
import com.pedidos360.orders.messaging.support.MessageRetryHandler;
import com.pedidos360.orders.messaging.support.NonRecoverableMessageException;
import com.pedidos360.orders.messaging.support.OrderEventReader;
import com.pedidos360.orders.service.OrderNotificationService;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.dao.DataAccessResourceFailureException;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OrderNotificationConsumerTest {

    private static final long TAG = 3L;
    private static final String VALID_EVENT = """
            {"eventId":"e-1","orderId":10,"userEmail":"cliente@test.cl","previousStatus":"RECIBIDO",
             "newStatus":"CONFIRMADO","total":1500,"occurredAt":"2026-10-28T12:00:00-03:00"}
            """;

    private OrderNotificationService notificationService;
    private MessageRetryHandler retryHandler;
    private Channel channel;
    private OrderNotificationConsumer consumer;

    @BeforeEach
    void setUp() {
        notificationService = mock(OrderNotificationService.class);
        retryHandler = mock(MessageRetryHandler.class);
        channel = mock(Channel.class);
        consumer = new OrderNotificationConsumer(new OrderEventReader(JsonMapper.builder().build()),
                notificationService, retryHandler, TestMessagingProperties.create());
    }

    @Test
    void procesaElEventoYHaceAck() throws Exception {
        consumer.onOrderEvent(message(VALID_EVENT), channel);

        verify(notificationService).notifyCustomer(any());
        verify(channel).basicAck(TAG, false);
        verifyNoInteractions(retryHandler);
    }

    @Test
    void jsonInvalidoSeRechazaHaciaLaDlqSinReintentar() throws Exception {
        Message message = message("esto no es json");

        consumer.onOrderEvent(message, channel);

        verify(retryHandler).reject(eq(message), eq(channel), eq("test.notifications"),
                any(NonRecoverableMessageException.class));
        verify(channel, never()).basicAck(TAG, false);
        verifyNoInteractions(notificationService);
    }

    @Test
    void pedidoInexistenteSeRechazaHaciaLaDlq() throws Exception {
        Message message = message(VALID_EVENT);
        doThrow(new ResourceNotFoundException("Orden no encontrada: 10"))
                .when(notificationService).notifyCustomer(any());

        consumer.onOrderEvent(message, channel);

        verify(retryHandler).reject(eq(message), eq(channel), eq("test.notifications"),
                any(ResourceNotFoundException.class));
    }

    @Test
    void errorTransitorioSeReintenta() throws Exception {
        Message message = message(VALID_EVENT);
        doThrow(new DataAccessResourceFailureException("BD caída"))
                .when(notificationService).notifyCustomer(any());

        consumer.onOrderEvent(message, channel);

        verify(retryHandler).retryOrReject(eq(message), eq(channel), eq("test.notifications"),
                any(DataAccessResourceFailureException.class));
        verify(channel, never()).basicAck(TAG, false);
    }

    private static Message message(String body) {
        MessageProperties props = new MessageProperties();
        props.setDeliveryTag(TAG);
        return new Message(body.getBytes(StandardCharsets.UTF_8), props);
    }
}
