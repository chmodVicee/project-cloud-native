package com.pedidos360.rabbitadmin.controller;

import com.pedidos360.rabbitadmin.dto.CreateExchangeRequest;
import com.pedidos360.rabbitadmin.dto.ExchangeResponse;
import com.pedidos360.rabbitadmin.dto.RabbitNames;
import com.pedidos360.rabbitadmin.service.RabbitAdminService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/** Administración de exchanges: POST /api/rabbit/exchanges y DELETE /api/rabbit/exchanges/{name}. */
@RestController
@RequestMapping("/api/rabbit/exchanges")
@RequiredArgsConstructor
public class ExchangeController {

    private final RabbitAdminService rabbitAdminService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ExchangeResponse create(@Valid @RequestBody CreateExchangeRequest request) {
        return rabbitAdminService.createExchange(request);
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> delete(@PathVariable
                                       @Pattern(regexp = RabbitNames.NAME_PATTERN, message = RabbitNames.NAME_MESSAGE)
                                       String name) {
        rabbitAdminService.deleteExchange(name);
        return ResponseEntity.noContent().build();
    }
}
