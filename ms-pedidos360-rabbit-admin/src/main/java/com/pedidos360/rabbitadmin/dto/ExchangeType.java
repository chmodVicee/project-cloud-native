package com.pedidos360.rabbitadmin.dto;

/** Tipos de exchange soportados por RabbitMQ. */
public enum ExchangeType {
    DIRECT, TOPIC, FANOUT, HEADERS;

    public String amqpName() {
        return name().toLowerCase();
    }
}
