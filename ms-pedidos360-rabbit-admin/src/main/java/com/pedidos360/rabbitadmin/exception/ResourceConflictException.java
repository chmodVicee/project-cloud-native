package com.pedidos360.rabbitadmin.exception;

/** El recurso ya existe o está protegido y no se puede modificar por la API. */
public class ResourceConflictException extends RuntimeException {

    public ResourceConflictException(String message) {
        super(message);
    }
}
