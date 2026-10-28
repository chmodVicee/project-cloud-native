package com.pedidos360.orders.messaging.support;

import com.pedidos360.orders.messaging.TestMessagingProperties;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.net.ConnectException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class MessageRetryHandlerTest {

    private static final long TAG = 7L;

    private RabbitTemplate rabbitTemplate;
    private Channel channel;
    private MessageRetryHandler handler;

    @BeforeEach
    void setUp() {
        rabbitTemplate = mock(RabbitTemplate.class);
        channel = mock(Channel.class);
        handler = new MessageRetryHandler(rabbitTemplate, TestMessagingProperties.create());
    }

    @Test
    void conIntentosDisponiblesReenviaALaColaDeEsperaYHaceAck() throws Exception {
        Message message = message(0);

        handler.retryOrReject(message, channel, "test.notifications", new RuntimeException("BD caída"));

        verify(rabbitTemplate).send("test.retry", "test.notifications", message);
        assertThat((Integer) message.getMessageProperties().getHeader(MessageRetryHandler.RETRY_COUNT_HEADER))
                .isEqualTo(1);
        verify(channel).basicAck(TAG, false);
        verify(channel, never()).basicNack(TAG, false, false);
    }

    @Test
    void alAgotarLosIntentosHaceNackSinRequeueHaciaLaDlq() throws Exception {
        Message message = message(2); // tercer intento con max-attempts = 3

        handler.retryOrReject(message, channel, "test.notifications", new RuntimeException("BD caída"));

        verify(channel).basicNack(TAG, false, false);
        verify(rabbitTemplate, never()).send(anyString(), anyString(), any(Message.class));
    }

    @Test
    void siNoSePuedeProgramarElReintentoEnviaALaDlq() throws Exception {
        Message message = message(0);
        doThrow(new AmqpConnectException(new ConnectException("sin broker")))
                .when(rabbitTemplate).send(eq("test.retry"), eq("test.notifications"), any(Message.class));

        handler.retryOrReject(message, channel, "test.notifications", new RuntimeException("BD caída"));

        verify(channel).basicNack(TAG, false, false);
        verify(channel, never()).basicAck(TAG, false);
    }

    @Test
    void rejectHaceNackSinRequeue() throws Exception {
        handler.reject(message(0), channel, "test.notifications", new NonRecoverableMessageException("JSON inválido"));

        verify(channel).basicNack(TAG, false, false);
    }

    private static Message message(int retries) {
        MessageProperties props = new MessageProperties();
        props.setDeliveryTag(TAG);
        props.setMessageId("msg-1");
        if (retries > 0) {
            props.setHeader(MessageRetryHandler.RETRY_COUNT_HEADER, retries);
        }
        return new Message("{}".getBytes(), props);
    }
}
