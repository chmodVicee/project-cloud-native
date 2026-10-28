package com.pedidos360.rabbitadmin.controller;

import com.pedidos360.rabbitadmin.exception.GlobalExceptionHandler;
import com.pedidos360.rabbitadmin.service.RabbitAdminService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Verifica que la API rechace con 400 los datos inválidos antes de llegar al servicio. */
class RabbitAdminControllerValidationTest {

    private RabbitAdminService service;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        service = mock(RabbitAdminService.class);
        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();
        mockMvc = MockMvcBuilders
                .standaloneSetup(new QueueController(service), new ExchangeController(service),
                        new BindingController(service))
                .setControllerAdvice(new GlobalExceptionHandler())
                .setValidator(validator)
                .build();
    }

    @Test
    void colaConNombreVacioEs400() throws Exception {
        mockMvc.perform(post("/api/rabbit/queues").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.name").exists());
        verifyNoInteractions(service);
    }

    @Test
    void colaConCaracteresInvalidosYTtlNegativoEs400() throws Exception {
        mockMvc.perform(post("/api/rabbit/queues").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"mi cola!\",\"messageTtlMs\":-5}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.name").exists())
                .andExpect(jsonPath("$.messageTtlMs").exists());
        verifyNoInteractions(service);
    }

    @Test
    void routingKeyDeDlqSinDlxEs400() throws Exception {
        mockMvc.perform(post("/api/rabbit/queues").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"tickets.queue\",\"deadLetterRoutingKey\":\"x\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.deadLetterConfigValid").exists());
        verifyNoInteractions(service);
    }

    @Test
    void exchangeSinTipoEs400() throws Exception {
        mockMvc.perform(post("/api/rabbit/exchanges").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"tickets.exchange\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").exists());
        verifyNoInteractions(service);
    }

    @Test
    void exchangeConTipoDesconocidoEs400() throws Exception {
        mockMvc.perform(post("/api/rabbit/exchanges").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"tickets.exchange\",\"type\":\"CIRCULAR\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").exists());
        verifyNoInteractions(service);
    }

    @Test
    void bindingSinDestinoEs400() throws Exception {
        mockMvc.perform(post("/api/rabbit/bindings").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"exchange\":\"tickets.exchange\",\"destinationType\":\"QUEUE\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.destination").exists());
        verifyNoInteractions(service);
    }

    @Test
    void eliminarBindingSinParametrosEs400() throws Exception {
        mockMvc.perform(delete("/api/rabbit/bindings"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.exchange").exists());
        verifyNoInteractions(service);
    }

    @Test
    void eliminarColaConNombreInvalidoEs400() throws Exception {
        mockMvc.perform(delete("/api/rabbit/queues/{name}", "cola con espacios"))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(service);
    }
}
