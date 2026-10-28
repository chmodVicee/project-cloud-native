package com.pedidos360.orders.config;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.GoogleCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * Configuración de Google Cloud Pub/Sub. Solo se activa con {@code messaging.pubsub.enabled=true}.
 *
 * <p>Ruta: cola RabbitMQ {@code queues.cloud-bridge} → {@code OrderCloudBridgeConsumer} →
 * topic {@code messaging.pubsub.topic} → suscripción {@code messaging.pubsub.subscription} →
 * {@code PubSubOrderStatusSubscriber}.
 *
 * <p>Con {@code emulator-host} definido se conecta al emulador por texto plano y sin
 * credenciales; si no, usa las credenciales por defecto de GCP (ADC).
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(PubSubProperties.class)
@ConditionalOnProperty(prefix = "messaging.pubsub", name = "enabled", havingValue = "true")
public class PubSubConfig {

    /** Canal gRPC en texto plano hacia el emulador; solo existe si hay emulator-host. */
    @Bean(destroyMethod = "shutdown")
    @ConditionalOnExpression("'${messaging.pubsub.emulator-host:}' != ''")
    public ManagedChannel pubSubEmulatorChannel(PubSubProperties properties) {
        return ManagedChannelBuilder.forTarget(properties.emulatorHost()).usePlaintext().build();
    }

    @Bean
    public TransportChannelProvider pubSubChannelProvider(ObjectProvider<ManagedChannel> pubSubEmulatorChannel) {
        ManagedChannel emulatorChannel = pubSubEmulatorChannel.getIfAvailable();
        if (emulatorChannel != null) {
            return FixedTransportChannelProvider.create(GrpcTransportChannel.create(emulatorChannel));
        }
        return TopicAdminSettings.defaultTransportChannelProvider();
    }

    @Bean
    public CredentialsProvider pubSubCredentialsProvider(PubSubProperties properties) {
        if (properties.usesEmulator()) {
            return NoCredentialsProvider.create();
        }
        return GoogleCredentialsProvider.newBuilder()
                .setScopesToApply(TopicAdminSettings.getDefaultServiceScopes())
                .build();
    }

    /** Publisher del topic de estados. Antes de crearlo asegura que existan topic y suscripción. */
    @Bean(destroyMethod = "shutdown")
    public Publisher orderStatusPublisher(PubSubProperties properties,
                                          TransportChannelProvider pubSubChannelProvider,
                                          CredentialsProvider pubSubCredentialsProvider) throws IOException {
        if (properties.createResources()) {
            createResourcesIfMissing(properties, pubSubChannelProvider, pubSubCredentialsProvider);
        }
        return Publisher.newBuilder(TopicName.of(properties.projectId(), properties.topic()))
                .setChannelProvider(pubSubChannelProvider)
                .setCredentialsProvider(pubSubCredentialsProvider)
                .build();
    }

    /**
     * Crea el topic y la suscripción si no existen. Si Pub/Sub no responde, la aplicación
     * arranca igual: los mensajes quedarán en reintento/DLQ del lado de RabbitMQ.
     */
    private static void createResourcesIfMissing(PubSubProperties properties,
                                                 TransportChannelProvider channelProvider,
                                                 CredentialsProvider credentialsProvider) {
        TopicName topic = TopicName.of(properties.projectId(), properties.topic());
        ProjectSubscriptionName subscription =
                ProjectSubscriptionName.of(properties.projectId(), properties.subscription());
        try (TopicAdminClient topics = TopicAdminClient.create(TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(credentialsProvider)
                .build());
             SubscriptionAdminClient subscriptions = SubscriptionAdminClient.create(SubscriptionAdminSettings.newBuilder()
                     .setTransportChannelProvider(channelProvider)
                     .setCredentialsProvider(credentialsProvider)
                     .build())) {
            try {
                topics.createTopic(topic);
                log.info("Topic de Pub/Sub creado: {}", topic);
            } catch (AlreadyExistsException ex) {
                log.debug("El topic {} ya existe", topic);
            }
            try {
                subscriptions.createSubscription(subscription, topic, PushConfig.getDefaultInstance(), 30);
                log.info("Suscripción de Pub/Sub creada: {}", subscription);
            } catch (AlreadyExistsException ex) {
                log.debug("La suscripción {} ya existe", subscription);
            }
        } catch (Exception ex) {
            log.error("No se pudieron crear los recursos de Pub/Sub ({}): {}", topic, ex.getMessage());
        }
    }
}
