package com.pedidos360.rabbitadmin.controller;

import com.pedidos360.rabbitadmin.dto.CreateQueueRequest;
import com.pedidos360.rabbitadmin.dto.QueueResponse;
import com.pedidos360.rabbitadmin.dto.RabbitNames;
import com.pedidos360.rabbitadmin.service.RabbitAdminService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;

/** Administración de colas: POST /api/rabbit/queues, GET y DELETE /api/rabbit/queues/{name}. */
@RestController
@RequestMapping("/api/rabbit/queues")
@RequiredArgsConstructor
public class QueueController {

    private final RabbitAdminService rabbitAdminService;

    @PostMapping
    public ResponseEntity<QueueResponse> create(@Valid @RequestBody CreateQueueRequest request) {
        QueueResponse created = rabbitAdminService.createQueue(request);
        return ResponseEntity.created(URI.create("/api/rabbit/queues/" + created.name())).body(created);
    }

    @GetMapping("/{name}")
    public QueueResponse get(@PathVariable
                             @Pattern(regexp = RabbitNames.NAME_PATTERN, message = RabbitNames.NAME_MESSAGE)
                             String name) {
        return rabbitAdminService.getQueue(name);
    }

    @DeleteMapping("/{name}")
    public ResponseEntity<Void> delete(@PathVariable
                                       @Pattern(regexp = RabbitNames.NAME_PATTERN, message = RabbitNames.NAME_MESSAGE)
                                       String name,
                                       @RequestParam(defaultValue = "false") boolean ifUnused,
                                       @RequestParam(defaultValue = "false") boolean ifEmpty) {
        rabbitAdminService.deleteQueue(name, ifUnused, ifEmpty);
        return ResponseEntity.noContent().build();
    }
}
