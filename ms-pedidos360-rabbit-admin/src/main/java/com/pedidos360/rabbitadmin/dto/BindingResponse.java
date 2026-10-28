package com.pedidos360.rabbitadmin.dto;

/** Binding creado. */
public record BindingResponse(String exchange, String destination, DestinationType destinationType,
                              String routingKey) {
}
