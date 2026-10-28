package com.pedidos360.rabbitadmin.dto;

/** Estado de una cola en el broker. */
public record QueueResponse(String name, long messageCount, int consumerCount) {
}
