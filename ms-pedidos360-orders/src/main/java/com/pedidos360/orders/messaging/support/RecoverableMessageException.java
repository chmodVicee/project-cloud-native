package com.pedidos360.orders.messaging.support;

/**
 * Error transitorio (base de datos o servicio externo no disponible). El mensaje se
 * reintenta hasta {@code messaging.rabbitmq.retry.max-attempts} veces y luego va a la DLQ.
 */
public class RecoverableMessageException extends RuntimeException {

    public RecoverableMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
