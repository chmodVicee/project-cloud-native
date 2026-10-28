package com.pedidos360.orders.service;

import com.pedidos360.orders.dto.CreateOrderRequest;
import com.pedidos360.orders.event.OrderStatusEvent;
import com.pedidos360.orders.exception.ResourceNotFoundException;
import com.pedidos360.orders.model.Order;
import com.pedidos360.orders.model.OrderItem;
import com.pedidos360.orders.model.OrderStatus;
import com.pedidos360.orders.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final RestClient restClient;
    private final ApplicationEventPublisher eventPublisher;

    @Value("${catalog.url}")
    private String catalogUrl;

    @Transactional
    public Order create(String userEmail, CreateOrderRequest request) {
        if (request.items() == null || request.items().isEmpty()) {
            throw new IllegalStateException("La orden debe tener al menos un producto");
        }

        Order order = Order.builder()
                .userEmail(userEmail)
                .status(OrderStatus.RECIBIDO)
                .createdAt(LocalDateTime.now(ZoneId.of("America/Santiago")))
                .total(BigDecimal.ZERO)
                .build();

        BigDecimal total = BigDecimal.ZERO;
        for (CreateOrderRequest.ItemLine line : request.items()) {
            ProductSnapshot product = fetchProduct(line.productId());

            if (product.stock() < line.quantity()) {
                throw new IllegalStateException("Stock insuficiente para el producto: " + product.name());
            }
            decrementStock(line.productId(), line.quantity());

            BigDecimal subtotal = product.price().multiply(BigDecimal.valueOf(line.quantity()));
            total = total.add(subtotal);

            OrderItem item = OrderItem.builder()
                    .productId(product.id())
                    .productName(product.name())
                    .price(product.price())
                    .quantity(line.quantity())
                    .subtotal(subtotal)
                    .build();
            order.addItem(item);
        }

        order.setTotal(total);
        Order saved = orderRepository.save(order);
        eventPublisher.publishEvent(OrderStatusEvent.of(saved, null));
        return saved;
    }

    public List<Order> findByUserEmail(String userEmail) {
        return orderRepository.findByUserEmail(userEmail);
    }

    public List<Order> findAll() {
        return orderRepository.findAll();
    }

    public Order findByIdAndUser(Long id, String userEmail) {
        Order order = orderRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Orden no encontrada: " + id));
        if (!order.getUserEmail().equals(userEmail)) {
            throw new ResourceNotFoundException("Orden no encontrada: " + id);
        }
        return order;
    }

    public Order findById(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Orden no encontrada: " + id));
    }

    @Transactional
    public Order updateStatus(Long id, OrderStatus newStatus) {
        Order order = findById(id);
        if (order.getStatus() == OrderStatus.CANCELADO || order.getStatus() == OrderStatus.ENTREGADO) {
            throw new IllegalStateException("No se puede cambiar el estado desde " + order.getStatus());
        }
        OrderStatus previousStatus = order.getStatus();
        order.setStatus(newStatus);
        Order saved = orderRepository.save(order);
        if (previousStatus != newStatus) {
            eventPublisher.publishEvent(OrderStatusEvent.of(saved, previousStatus));
        }
        return saved;
    }

    private ProductSnapshot fetchProduct(Long productId) {
        try {
            return restClient.get()
                    .uri(catalogUrl + "/api/catalog/products/{id}", productId)
                    .retrieve()
                    .body(ProductSnapshot.class);
        } catch (HttpClientErrorException ex) {
            if (ex.getStatusCode().value() == 404) {
                throw new ResourceNotFoundException("Producto no disponible: " + productId);
            }
            throw new IllegalStateException("Error consultando el producto " + productId);
        }
    }

    private void decrementStock(Long productId, int quantity) {
        restClient.patch()
                .uri(catalogUrl + "/api/catalog/products/{id}/stock", productId)
                .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                .body(Map.of("delta", -quantity))
                .retrieve()
                .toBodilessEntity();
    }
}