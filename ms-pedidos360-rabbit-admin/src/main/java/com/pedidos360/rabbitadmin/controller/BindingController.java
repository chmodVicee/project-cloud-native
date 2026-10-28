package com.pedidos360.rabbitadmin.controller;

import com.pedidos360.rabbitadmin.dto.BindingRequest;
import com.pedidos360.rabbitadmin.dto.BindingResponse;
import com.pedidos360.rabbitadmin.service.RabbitAdminService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Administración de bindings: POST /api/rabbit/bindings (cuerpo JSON) y
 * DELETE /api/rabbit/bindings?exchange=..&amp;destination=..&amp;destinationType=..&amp;routingKey=..
 */
@RestController
@RequestMapping("/api/rabbit/bindings")
@RequiredArgsConstructor
public class BindingController {

    private final RabbitAdminService rabbitAdminService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public BindingResponse create(@Valid @RequestBody BindingRequest request) {
        return rabbitAdminService.createBinding(request);
    }

    @DeleteMapping
    public ResponseEntity<Void> delete(@Valid @ModelAttribute BindingRequest request) {
        rabbitAdminService.deleteBinding(request);
        return ResponseEntity.noContent().build();
    }
}
