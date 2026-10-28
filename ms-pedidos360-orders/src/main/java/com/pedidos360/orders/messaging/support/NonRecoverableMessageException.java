package com.pedidos360.orders.messaging.support;

/**
 * Error que no se arregla reintentando (mensaje mal formado, pedido inexistente...).
 * El consumidor responde NACK sin requeue y el mensaje termina en la DLQ.
 */
public class NonRecoverableMessageException extends RuntimeException {

    public NonRecoverableMessageException(String message) {
        super(message);
    }

    public NonRecoverableMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
