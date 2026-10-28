package com.pedidos360.rabbitadmin.service;

import com.pedidos360.rabbitadmin.config.RabbitAdminProperties;
import com.pedidos360.rabbitadmin.dto.BindingRequest;
import com.pedidos360.rabbitadmin.dto.BindingResponse;
import com.pedidos360.rabbitadmin.dto.CreateExchangeRequest;
import com.pedidos360.rabbitadmin.dto.CreateQueueRequest;
import com.pedidos360.rabbitadmin.dto.DestinationType;
import com.pedidos360.rabbitadmin.dto.ExchangeResponse;
import com.pedidos360.rabbitadmin.dto.QueueResponse;
import com.pedidos360.rabbitadmin.exception.ResourceConflictException;
import com.pedidos360.rabbitadmin.exception.ResourceNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

/**
 * Encapsula toda la administración de RabbitMQ (colas, exchanges y bindings). Los
 * controladores solo llaman a estos métodos y no conocen la API de Spring AMQP.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RabbitAdminService {

    private final AmqpAdmin amqpAdmin;
    private final RabbitTemplate rabbitTemplate;
    private final RabbitAdminProperties properties;

    // ---------- Colas ----------

    public QueueResponse createQueue(CreateQueueRequest request) {
        assertNotProtected(request.name(), "la cola");
        if (amqpAdmin.getQueueInfo(request.name()) != null) {
            throw new ResourceConflictException("La cola '" + request.name() + "' ya existe");
        }
        if (request.deadLetterExchange() != null) {
            assertExchangeExists(request.deadLetterExchange());
        }

        QueueBuilder builder = request.isDurable()
                ? QueueBuilder.durable(request.name())
                : QueueBuilder.nonDurable(request.name());
        if (request.isAutoDelete()) {
            builder.autoDelete();
        }
        if (request.deadLetterExchange() != null) {
            builder.deadLetterExchange(request.deadLetterExchange());
        }
        if (request.deadLetterRoutingKey() != null) {
            builder.deadLetterRoutingKey(request.deadLetterRoutingKey());
        }
        if (request.messageTtlMs() != null) {
            builder.ttl(request.messageTtlMs().intValue());
        }
        if (request.maxLength() != null) {
            builder.maxLength(request.maxLength());
        }
        Queue queue = builder.build();
        amqpAdmin.declareQueue(queue);
        log.info("Cola creada: {} (durable={}, argumentos={})", queue.getName(), queue.isDurable(), queue.getArguments());
        return getQueue(queue.getName());
    }

    public QueueResponse getQueue(String name) {
        QueueInformation info = amqpAdmin.getQueueInfo(name);
        if (info == null) {
            throw new ResourceNotFoundException("La cola '" + name + "' no existe");
        }
        return new QueueResponse(info.getName(), info.getMessageCount(), info.getConsumerCount());
    }

    /**
     * @param ifUnused solo elimina si no tiene consumidores
     * @param ifEmpty  solo elimina si no tiene mensajes
     */
    public void deleteQueue(String name, boolean ifUnused, boolean ifEmpty) {
        assertNotProtected(name, "la cola");
        QueueResponse queue = getQueue(name);
        if (ifUnused && queue.consumerCount() > 0) {
            throw new ResourceConflictException("La cola '" + name + "' tiene " + queue.consumerCount() + " consumidores");
        }
        if (ifEmpty && queue.messageCount() > 0) {
            throw new ResourceConflictException("La cola '" + name + "' tiene " + queue.messageCount() + " mensajes");
        }
        amqpAdmin.deleteQueue(name, ifUnused, ifEmpty);
        log.info("Cola eliminada: {}", name);
    }

    // ---------- Exchanges ----------

    public ExchangeResponse createExchange(CreateExchangeRequest request) {
        assertNotProtected(request.name(), "el exchange");
        if (exchangeExists(request.name())) {
            throw new ResourceConflictException("El exchange '" + request.name() + "' ya existe");
        }
        Exchange exchange = new ExchangeBuilder(request.name(), request.type().amqpName())
                .durable(request.isDurable())
                .build();
        amqpAdmin.declareExchange(exchange);
        log.info("Exchange creado: {} ({})", exchange.getName(), exchange.getType());
        return new ExchangeResponse(exchange.getName(), request.type(), exchange.isDurable());
    }

    public void deleteExchange(String name) {
        assertNotProtected(name, "el exchange");
        assertExchangeExists(name);
        amqpAdmin.deleteExchange(name);
        log.info("Exchange eliminado: {}", name);
    }

    // ---------- Bindings ----------

    public BindingResponse createBinding(BindingRequest request) {
        assertNotProtected(request.exchange(), "el exchange");
        assertNotProtected(request.destination(), "el destino");
        assertBindingEndpointsExist(request);
        amqpAdmin.declareBinding(toBinding(request));
        log.info("Binding creado: {} --({})--> {} {}", request.exchange(), request.routingKeyOrEmpty(),
                request.destinationType(), request.destination());
        return new BindingResponse(request.exchange(), request.destination(), request.destinationType(),
                request.routingKeyOrEmpty());
    }

    public void deleteBinding(BindingRequest request) {
        assertNotProtected(request.exchange(), "el exchange");
        assertNotProtected(request.destination(), "el destino");
        assertBindingEndpointsExist(request);
        amqpAdmin.removeBinding(toBinding(request));
        log.info("Binding eliminado: {} --({})--> {} {}", request.exchange(), request.routingKeyOrEmpty(),
                request.destinationType(), request.destination());
    }

    // ---------- Validaciones contra el broker ----------

    private void assertNotProtected(String name, String resource) {
        if (properties.isProtected(name)) {
            throw new ResourceConflictException("No se puede modificar " + resource + " '" + name
                    + "': usa un prefijo protegido " + properties.protectedPrefixes());
        }
    }

    private void assertBindingEndpointsExist(BindingRequest request) {
        assertExchangeExists(request.exchange());
        if (request.destinationType() == DestinationType.QUEUE) {
            getQueue(request.destination());
        } else {
            assertExchangeExists(request.destination());
        }
    }

    private void assertExchangeExists(String name) {
        if (!exchangeExists(name)) {
            throw new ResourceNotFoundException("El exchange '" + name + "' no existe");
        }
    }

    /** Declaración pasiva: el broker responde error si el exchange no existe. */
    private boolean exchangeExists(String name) {
        try {
            rabbitTemplate.execute(channel -> channel.exchangeDeclarePassive(name));
            return true;
        } catch (AmqpConnectException ex) {
            throw ex;
        } catch (AmqpException ex) {
            return false;
        }
    }

    private static Binding toBinding(BindingRequest request) {
        Binding.DestinationType type = request.destinationType() == DestinationType.QUEUE
                ? Binding.DestinationType.QUEUE
                : Binding.DestinationType.EXCHANGE;
        return new Binding(request.destination(), type, request.exchange(), request.routingKeyOrEmpty(), null);
    }
}
