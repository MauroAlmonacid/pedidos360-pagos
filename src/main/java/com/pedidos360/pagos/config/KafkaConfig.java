package com.pedidos360.pagos.config;

import com.pedidos360.pagos.dto.IntentoPagoDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaConfig {

    @Bean // Configura cómo se serializan los eventos de pago antes de enviarlos a Kafka
    public ProducerFactory<String, IntentoPagoDTO> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean // Herramienta principal que usará el servicio para publicar eventos en el tópico
    public KafkaTemplate<String, IntentoPagoDTO> kafkaTemplate(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return new KafkaTemplate<>(producerFactory(bootstrapServers));
    }

    @Bean // Control de reintentos (2 reintentos cada 1 segundo) si llega un mensaje corrupto a Kafka
    public DefaultErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler((record, exception) -> {
            log.error("Mensaje corrupto descartado tras reintentos en tópico {}: {}",
                    record.topic(), exception.getMessage());
        }, new FixedBackOff(1000L, 2L));
    }
}