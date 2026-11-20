package com.pedidos360.kafka.consumer;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaMessageConsumerTest {

    private final KafkaMessageConsumer consumer = new KafkaMessageConsumer();
    private final Logger logger = (Logger) LoggerFactory.getLogger(KafkaMessageConsumer.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    @BeforeEach
    void setUp() {
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(appender);
        appender.stop();
    }

    @Test
    void registraParticionOffsetKeyYMensaje() {
        consumer.listen("mensaje-1", 2, 17L, "orden-1");

        assertThat(appender.list)
                .extracting(ILoggingEvent::getFormattedMessage)
                .anyMatch(log -> log.contains("particion=2")
                        && log.contains("offset=17")
                        && log.contains("key=orden-1")
                        && log.contains("mensaje=mensaje-1"));
    }

    @Test
    void indicaCuandoElMensajeNoTieneKey() {
        consumer.listen("mensaje-2", 0, 1L, null);

        assertThat(appender.list)
                .extracting(ILoggingEvent::getFormattedMessage)
                .anyMatch(log -> log.contains("key=(sin key)"));
    }
}
