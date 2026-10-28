package com.pedidos360.rabbitadmin.dto;

/** Reglas de formato compartidas por los DTO de la API. */
public final class RabbitNames {

    /** Nombre de cola o exchange: 1 a 255 caracteres, letras, números y . _ : - */
    public static final String NAME_PATTERN = "^[A-Za-z0-9._:-]{1,255}$";
    public static final String NAME_MESSAGE =
            "debe tener entre 1 y 255 caracteres y solo letras, números, '.', '_', ':' o '-'";

    /** Routing key: puede ser vacía (fanout) y admite los comodines * y # de los topic. */
    public static final String ROUTING_KEY_PATTERN = "^[A-Za-z0-9._:*#-]{0,255}$";
    public static final String ROUTING_KEY_MESSAGE =
            "debe tener hasta 255 caracteres y solo letras, números, '.', '_', ':', '-', '*' o '#'";

    private RabbitNames() {
    }
}
