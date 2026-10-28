package com.pedidos360.orders.messaging.consumer.cloud;

import com.pedidos360.orders.messaging.TestMessagingProperties;
import com.pedidos360.orders.messaging.producer.cloud.CloudStatusPublisher;
import com.pedidos360.orders.messaging.support.MessageRetryHandler;
import com.pedidos360.orders.messaging.support.NonRecoverableMessageException;
import com.pedidos360.orders.messaging.support.OrderEventReader;
import com.pedidos360.orders.messaging.support.RecoverableMessageException;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class OrderCloudBridgeConsumerTest {

    private static final long TAG = 5L;
    private static final String VALID_EVENT = """
            {"eventId":"e-1","orderId":10,"userEmail":"cliente@test.cl","previousStatus":"LISTO",
             "newStatus":"ENTREGADO","total":1500,"occurredAt":"2026-10-28T12:00:00-03:00"}
            """;

    private CloudStatusPublisher cloudPublisher;
    private MessageRetryHandler retryHandler;
    private Channel channel;
    private OrderCloudBridgeConsumer consumer;

    @BeforeEach
    void setUp() {
        cloudPublisher = mock(CloudStatusPublisher.class);
        retryHandler = mock(MessageRetryHandler.class);
        channel = mock(Channel.class);
        consumer = new OrderCloudBridgeConsumer(new OrderEventReader(JsonMapper.builder().build()),
                cloudPublisher, retryHandler, TestMessagingProperties.create());
    }

    @Test
    void reenviaAPubSubYHaceAck() throws Exception {
        Message message = message(VALID_EVENT);

        consumer.onOrderEvent(message, channel);

        verify(cloudPublisher).publish(any(), eq(message.getBody()));
        verify(channel).basicAck(TAG, false);
        verifyNoInteractions(retryHandler);
    }

    @Test
    void eventoIncompletoSeRechazaHaciaLaDlq() throws Exception {
        Message message = message("{\"orderId\":10}");

        consumer.onOrderEvent(message, channel);

        verify(retryHandler).reject(eq(message), eq(channel), eq("test.cloud"),
                any(NonRecoverableMessageException.class));
        verifyNoInteractions(cloudPublisher);
    }

    @Test
    void pubSubCaidoSeReintenta() throws Exception {
        Message message = message(VALID_EVENT);
        doThrow(new RecoverableMessageException("sin respuesta", new TimeoutException()))
                .when(cloudPublisher).publish(any(), any());

        consumer.onOrderEvent(message, channel);

        verify(retryHandler).retryOrReject(eq(message), eq(channel), eq("test.cloud"),
                any(RecoverableMessageException.class));
        verify(channel, never()).basicAck(TAG, false);
    }

    private static Message message(String body) {
        MessageProperties props = new MessageProperties();
        props.setDeliveryTag(TAG);
        return new Message(body.getBytes(StandardCharsets.UTF_8), props);
    }
}
