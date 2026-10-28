package com.pedidos360.orders.messaging.consumer.cloud;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.pedidos360.orders.config.PubSubProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Subscriber de Pub/Sub (consumidor del lado de la nube): recibe los estados reenviados
 * desde RabbitMQ y los registra como auditoría. Hace ACK si el mensaje trae datos y NACK
 * (Pub/Sub lo vuelve a entregar) si viene vacío o falla el procesamiento.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "messaging.pubsub", name = "enabled", havingValue = "true")
public class PubSubOrderStatusSubscriber implements SmartLifecycle {

    private final PubSubProperties properties;
    private final TransportChannelProvider pubSubChannelProvider;
    private final CredentialsProvider pubSubCredentialsProvider;

    private Subscriber subscriber;

    @Override
    public void start() {
        ProjectSubscriptionName subscription =
                ProjectSubscriptionName.of(properties.projectId(), properties.subscription());
        subscriber = Subscriber.newBuilder(subscription, this::receive)
                .setChannelProvider(pubSubChannelProvider)
                .setCredentialsProvider(pubSubCredentialsProvider)
                .build();
        subscriber.addListener(new Subscriber.Listener() {
            @Override
            public void failed(Subscriber.State from, Throwable failure) {
                log.error("El subscriber de Pub/Sub {} se detuvo: {}", subscription, failure.getMessage());
            }
        }, Runnable::run);
        subscriber.startAsync();
        log.info("Subscriber de Pub/Sub escuchando {}", subscription);
    }

    void receive(PubsubMessage message, AckReplyConsumer reply) {
        try {
            if (message.getData().isEmpty()) {
                log.warn("[PUBSUB] mensaje {} sin datos; NACK", message.getMessageId());
                reply.nack();
                return;
            }
            log.info("[PUBSUB] estado recibido pedido #{} -> {} (id {}): {}",
                    message.getAttributesOrDefault("orderId", "?"),
                    message.getAttributesOrDefault("newStatus", "?"),
                    message.getMessageId(),
                    message.getData().toStringUtf8());
            reply.ack();
        } catch (RuntimeException ex) {
            log.error("[PUBSUB] error procesando el mensaje {}; NACK", message.getMessageId(), ex);
            reply.nack();
        }
    }

    @Override
    public void stop() {
        if (subscriber != null) {
            try {
                subscriber.stopAsync().awaitTerminated(5, TimeUnit.SECONDS);
            } catch (Exception ex) {
                log.warn("No se pudo detener el subscriber de Pub/Sub limpiamente: {}", ex.getMessage());
            }
        }
    }

    @Override
    public boolean isRunning() {
        return subscriber != null && subscriber.isRunning();
    }
}
