package com.pedidos360.kafka.controller;

import com.pedidos360.kafka.producer.KafkaMessageProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Endpoint REST de la actividad: publica mensajes en el topic Kafka. */
@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaController {

    private final KafkaMessageProducer producer;

    @RequestMapping(value = "/send", method = {RequestMethod.GET, RequestMethod.POST})
    public String sendMessage(@RequestParam String message,
                              @RequestParam(required = false) String key) {
        producer.send(key, message);
        return "Mensaje enviado: " + message + " | Key: " + key;
    }
}
