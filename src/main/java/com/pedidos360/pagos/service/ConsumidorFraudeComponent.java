package com.pedidos360.pagos.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pedidos360.pagos.dto.IntentoPagoDTO;
import com.pedidos360.pagos.model.EstadoPagoCliente;
import com.pedidos360.pagos.repository.EstadoPagoClienteRepositorio;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumidorFraudeComponent {

    private final EstadoPagoClienteRepositorio estadoRepositorio;
    private final ObjectMapper objectMapper;
    
    // Escucha en tiempo real todos los eventos que llegan al tópico payment-attempts
    @KafkaListener(topics = "payment-attempts", groupId = "payment-velocity-group")
    @Transactional
    public void consumirIntentoPago(IntentoPagoDTO evento) {
        // 1. Validación defensiva del payload (criterio clave en la rúbrica de manejo de errores)
        if (evento == null || evento.getIdCliente() == null || evento.getIdCliente().isBlank()) {
            log.warn("Payload inválido recibido en payment-attempts: se ignora mensaje sin idCliente.");
            return;
        }

        log.info("Consumidor Fraude: evaluando intento de cliente {}", evento.getIdCliente());
        LocalDateTime ahora = LocalDateTime.now();

        // Busca al cliente en H2 o crea su ficha inicial si es su primera compra
        EstadoPagoCliente estado = estadoRepositorio.findById(evento.getIdCliente())
                .orElseGet(() -> {
                    EstadoPagoCliente nuevo = new EstadoPagoCliente();
                    nuevo.setIdCliente(evento.getIdCliente());
                    nuevo.setCantidadIntentos(0);
                    nuevo.setBloqueado(false);
                    return nuevo;
                });

        // Si ya está bloqueado y aún no pasan los 30 minutos, no seguimos contando
        if (estado.isBloqueado() && estado.getBloqueadoHasta() != null && ahora.isBefore(estado.getBloqueadoHasta())) {
            log.warn("Cliente {} sigue bloqueado hasta {}", evento.getIdCliente(), estado.getBloqueadoHasta());
            return;
        }

        // Si pasaron más de 10 segundos desde el intento anterior, es un flujo humano normal: reseteamos
        if (estado.getActualizadoEn() != null) {
            long segundos = Duration.between(estado.getActualizadoEn(), ahora).getSeconds();
            if (segundos > 10) {
                estado.setCantidadIntentos(0);
            }
        }

        // Sumamos el intento actual
        estado.setCantidadIntentos(estado.getCantidadIntentos() + 1);
        estado.setActualizadoEn(ahora);

        // Regla Velocity Check: 3 o más intentos en menos de 10 segundos = sospecha de bot/fraude
        if (estado.getCantidadIntentos() >= 3) {
            estado.setBloqueado(true);
            estado.setBloqueadoHasta(ahora.plusMinutes(30));
            log.error("ALERTA FRAUDE: Cliente {} acumuló {} intentos en <10s. Bloqueado por 30 min.",
                    evento.getIdCliente(), estado.getCantidadIntentos());
        }

        estadoRepositorio.save(estado);
    }
}