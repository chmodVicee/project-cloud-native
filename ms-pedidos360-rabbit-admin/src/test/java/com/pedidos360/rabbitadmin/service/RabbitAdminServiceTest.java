package com.pedidos360.rabbitadmin.service;

import com.pedidos360.rabbitadmin.config.RabbitAdminProperties;
import com.pedidos360.rabbitadmin.dto.BindingRequest;
import com.pedidos360.rabbitadmin.dto.CreateExchangeRequest;
import com.pedidos360.rabbitadmin.dto.CreateQueueRequest;
import com.pedidos360.rabbitadmin.dto.DestinationType;
import com.pedidos360.rabbitadmin.dto.ExchangeType;
import com.pedidos360.rabbitadmin.exception.ResourceConflictException;
import com.pedidos360.rabbitadmin.exception.ResourceNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RabbitAdminServiceTest {

    private AmqpAdmin amqpAdmin;
    private RabbitTemplate rabbitTemplate;
    private RabbitAdminService service;

    @BeforeEach
    void setUp() {
        amqpAdmin = mock(AmqpAdmin.class);
        rabbitTemplate = mock(RabbitTemplate.class);
        service = new RabbitAdminService(amqpAdmin, rabbitTemplate,
                new RabbitAdminProperties(List.of("amq.", "pedidos360.")));
    }

    @Test
    void creaColaConDlqYTtl() {
        when(amqpAdmin.getQueueInfo("tickets.queue"))
                .thenReturn(null)
                .thenReturn(new QueueInformation("tickets.queue", 0, 0));
        exchangeExists(true);

        service.createQueue(new CreateQueueRequest("tickets.queue", true, false,
                "tickets.dlx", "tickets.dlq", 60000L, 100));

        ArgumentCaptor<Queue> captor = ArgumentCaptor.forClass(Queue.class);
        verify(amqpAdmin).declareQueue(captor.capture());
        assertThat(captor.getValue().getArguments())
                .containsEntry("x-dead-letter-exchange", "tickets.dlx")
                .containsEntry("x-dead-letter-routing-key", "tickets.dlq")
                .containsEntry("x-message-ttl", 60000)
                .containsEntry("x-max-length", 100L);
    }

    @Test
    void noCreaUnaColaQueYaExiste() {
        when(amqpAdmin.getQueueInfo("tickets.queue")).thenReturn(new QueueInformation("tickets.queue", 0, 0));

        assertThatThrownBy(() -> service.createQueue(simpleQueue("tickets.queue")))
                .isInstanceOf(ResourceConflictException.class);
        verify(amqpAdmin, never()).declareQueue(any());
    }

    @Test
    void noPermiteTocarRecursosProtegidos() {
        assertThatThrownBy(() -> service.createQueue(simpleQueue("amq.mia")))
                .isInstanceOf(ResourceConflictException.class);
        assertThatThrownBy(() -> service.deleteExchange("pedidos360.orders.exchange"))
                .isInstanceOf(ResourceConflictException.class);
    }

    @Test
    void eliminarColaInexistenteDevuelveNoEncontrado() {
        assertThatThrownBy(() -> service.deleteQueue("no.existe", false, false))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    void eliminarColaConMensajesYIfEmptyEsConflicto() {
        when(amqpAdmin.getQueueInfo("tickets.queue")).thenReturn(new QueueInformation("tickets.queue", 5, 0));

        assertThatThrownBy(() -> service.deleteQueue("tickets.queue", false, true))
                .isInstanceOf(ResourceConflictException.class);
        verify(amqpAdmin, never()).deleteQueue(any(), any(Boolean.class), any(Boolean.class));
    }

    @Test
    void creaExchangeDelTipoIndicado() {
        exchangeExists(false);

        service.createExchange(new CreateExchangeRequest("tickets.exchange", ExchangeType.FANOUT, null));

        ArgumentCaptor<Exchange> captor = ArgumentCaptor.forClass(Exchange.class);
        verify(amqpAdmin).declareExchange(captor.capture());
        assertThat(captor.getValue().getType()).isEqualTo("fanout");
        assertThat(captor.getValue().isDurable()).isTrue();
    }

    @Test
    void bindingHaciaColaInexistenteDevuelveNoEncontrado() {
        exchangeExists(true);

        assertThatThrownBy(() -> service.createBinding(
                new BindingRequest("tickets.exchange", "no.existe", DestinationType.QUEUE, "tickets.#")))
                .isInstanceOf(ResourceNotFoundException.class);
        verify(amqpAdmin, never()).declareBinding(any());
    }

    @Test
    void creaBindingEntreExchangeYCola() {
        exchangeExists(true);
        when(amqpAdmin.getQueueInfo("tickets.queue")).thenReturn(new QueueInformation("tickets.queue", 0, 0));

        service.createBinding(new BindingRequest("tickets.exchange", "tickets.queue", DestinationType.QUEUE, "tickets.#"));

        ArgumentCaptor<Binding> captor = ArgumentCaptor.forClass(Binding.class);
        verify(amqpAdmin).declareBinding(captor.capture());
        assertThat(captor.getValue().getExchange()).isEqualTo("tickets.exchange");
        assertThat(captor.getValue().getDestination()).isEqualTo("tickets.queue");
        assertThat(captor.getValue().getRoutingKey()).isEqualTo("tickets.#");
    }

    @SuppressWarnings("unchecked")
    private void exchangeExists(boolean exists) {
        if (exists) {
            when(rabbitTemplate.execute(any(ChannelCallback.class))).thenReturn(null);
        } else {
            when(rabbitTemplate.execute(any(ChannelCallback.class)))
                    .thenThrow(new AmqpIOException(new IOException("NOT_FOUND")));
        }
    }

    private static CreateQueueRequest simpleQueue(String name) {
        return new CreateQueueRequest(name, null, null, null, null, null, null);
    }
}
