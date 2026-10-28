package com.pedidos360.rabbitadmin.dto;

/** Exchange creado. */
public record ExchangeResponse(String name, ExchangeType type, boolean durable) {
}
