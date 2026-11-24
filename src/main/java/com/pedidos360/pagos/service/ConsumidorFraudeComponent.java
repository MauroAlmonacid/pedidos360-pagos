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
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumidorFraudeComponent {

    // Repositorio H2 donde guardamos cuántos intentos lleva cada cliente y si está bloqueado
    private final EstadoPagoClienteRepositorio repositorio;

    // Traductor que convierte el texto JSON recibido desde Kafka en nuestro objeto IntentoPagoDTO
    private final ObjectMapper objectMapper;

    // Escucha el tópico "payment-attempts" recibiendo el mensaje como texto plano (String)
    @KafkaListener(topics = "payment-attempts", groupId = "payment-velocity-group")
    @Transactional
    public void consumirIntentoPago(String mensajeJson) {
        IntentoPagoDTO evento;
        try {
            // Convierte el texto JSON entrante en el objeto IntentoPagoDTO
            evento = objectMapper.readValue(mensajeJson, IntentoPagoDTO.class);
        } catch (Exception e) {
            log.error("JSON inválido recibido en payment-attempts: {}", e.getMessage());
            return;
        }

        // Ignora mensajes vacíos o sin identificador de cliente
        if (evento == null || evento.getIdCliente() == null || evento.getIdCliente().isBlank()) {
            log.warn("Payload sin idCliente ignorado.");
            return;
        }

        String idCliente = evento.getIdCliente();
        LocalDateTime ahora = LocalDateTime.now();
        log.info("Consumidor Fraude: evaluando intento de cliente {}", idCliente);

        // Busca si el cliente ya tiene historial de intentos en la base de datos H2
        Optional<EstadoPagoCliente> registroOpt = repositorio.findById(idCliente);

        if (registroOpt.isEmpty()) {
            // Primer intento registrado para este cliente: lo guardamos con contador en 1
            EstadoPagoCliente nuevo = new EstadoPagoCliente();
            nuevo.setIdCliente(idCliente);
            nuevo.setCantidadIntentos(1);
            nuevo.setBloqueado(false);
            nuevo.setBloqueadoHasta(null);
            nuevo.setActualizadoEn(ahora);
            repositorio.save(nuevo);
            log.info("Cliente {} registrado con 1 intento.", idCliente);
            return;
        }

        EstadoPagoCliente estado = registroOpt.get();

        // Si ya está bloqueado y aún no pasan los 30 minutos, no hacemos nada más
        if (estado.isBloqueado() && estado.getBloqueadoHasta() != null && ahora.isBefore(estado.getBloqueadoHasta())) {
            log.warn("Cliente {} ya se encuentra bloqueado hasta {}", idCliente, estado.getBloqueadoHasta());
            return;
        }

        // Calculamos cuántos segundos pasaron desde el intento anterior
        long segundosTranscurridos = (estado.getActualizadoEn() != null)
                ? Duration.between(estado.getActualizadoEn(), ahora).getSeconds()
                : 999;

        if (segundosTranscurridos >= 10) {
            // Pasaron 10 segundos o más: reiniciamos la ventana de conteo a 1 intento
            estado.setCantidadIntentos(1);
            estado.setBloqueado(false);
            estado.setBloqueadoHasta(null);
            estado.setActualizadoEn(ahora);
            repositorio.save(estado);
            log.info("Ventana reiniciada para cliente {} (intento 1).", idCliente);
            return;
        }

        // Menos de 10 segundos desde el último intento: sumamos 1 al contador
        int nuevosIntentos = estado.getCantidadIntentos() + 1;
        estado.setCantidadIntentos(nuevosIntentos);
        estado.setActualizadoEn(ahora);

        // Al llegar a 3 intentos en menos de 10 segundos, activamos el bloqueo de 30 minutos
        if (nuevosIntentos >= 3) {
            estado.setBloqueado(true);
            estado.setBloqueadoHasta(ahora.plusMinutes(30));
            log.error("ALERTA FRAUDE: Cliente {} acumuló {} intentos en <10s. Bloqueado por 30 min.",
                    idCliente, nuevosIntentos);
        } else {
            log.info("Cliente {} lleva {} intentos en la ventana actual.", idCliente, nuevosIntentos);
        }

        repositorio.save(estado);
    }
}