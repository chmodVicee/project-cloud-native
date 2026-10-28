package com.pedidos360.orders.service;

import com.pedidos360.orders.event.OrderStatusEvent;
import com.pedidos360.orders.exception.ResourceNotFoundException;
import com.pedidos360.orders.model.OrderStatus;
import com.pedidos360.orders.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Tarea asíncrona de negocio: avisar al cliente que su pedido cambió de estado.
 * La "notificación" se registra en el log (no hay proveedor de correo configurado).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderNotificationService {

    private final OrderRepository orderRepository;

    public void notifyCustomer(OrderStatusEvent event) {
        if (!orderRepository.existsById(event.orderId())) {
            throw new ResourceNotFoundException("Orden no encontrada: " + event.orderId());
        }
        log.info("[NOTIFICACIÓN] Para {}: {}", event.userEmail(), buildText(event));
    }

    private static String buildText(OrderStatusEvent event) {
        if (event.isCreation()) {
            return "recibimos tu pedido #" + event.orderId() + " por $" + event.total() + ".";
        }
        if (event.newStatus() == OrderStatus.CANCELADO) {
            return "tu pedido #" + event.orderId() + " fue cancelado.";
        }
        return "tu pedido #" + event.orderId() + " pasó de " + event.previousStatus()
                + " a " + event.newStatus() + ".";
    }
}
