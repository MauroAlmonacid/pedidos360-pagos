package com.pedidos360.pagos.service;

import cl.transbank.common.IntegrationApiKeys;
import cl.transbank.webpay.webpayplus.WebpayPlus;
import com.pedidos360.pagos.dto.IntentoPagoDTO;
import com.pedidos360.pagos.dto.TransaccionRespuestaDTO;
import com.pedidos360.pagos.model.EstadoPagoCliente;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pedidos360.pagos.repository.EstadoPagoClienteRepositorio;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class PagosService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final EstadoPagoClienteRepositorio estadoRepositorio;
    private static final String TOPIC_NOMBRE = "payment-attempts"; // Tópico creado en Fase 1

    // Revisa si el cliente está castigado; si ya pasaron los 30 minutos, lo perdona automáticamente
    public boolean verificarSiUsuarioEstaBloqueado(String idCliente) {
        EstadoPagoCliente estado = estadoRepositorio.findById(idCliente).orElse(null);

        if (estado == null || !estado.isBloqueado()) {
            return false; // No tiene historial o no está bloqueado
        }

        if (estado.getBloqueadoHasta() != null) {
            LocalDateTime ahora = LocalDateTime.now();
            if (ahora.isAfter(estado.getBloqueadoHasta())) {
                // Ya expiró el castigo de 30 minutos: reiniciamos su estado
                estado.setBloqueado(false);
                estado.setCantidadIntentos(0);
                estado.setBloqueadoHasta(null);
                estado.setActualizadoEn(ahora);
                estadoRepositorio.save(estado);
                log.info("Bloqueo expirado para cliente {}. Desbloqueado automáticamente.", idCliente);
                return false;
            }
        }
        return true; // Sigue dentro de los 30 minutos de castigo
    }

    // Envía el intento de pago de forma asíncrona a Kafka usando idCliente como Key de partición
    public void producirIntentoPago(String idCliente, Long monto, String idTransaccion) {
        try {
            IntentoPagoDTO evento = new IntentoPagoDTO(idCliente, monto, idTransaccion, LocalDateTime.now());
            String jsonEvento = objectMapper.writeValueAsString(evento);
            kafkaTemplate.send(TOPIC_NOMBRE, idCliente, jsonEvento);
            log.info("Evento de intento enviado a Kafka [{}] para cliente: {}", TOPIC_NOMBRE, idCliente);
        } catch (Exception e) {
            log.error("Error al serializar evento hacia Kafka: {}", e.getMessage());
        }
    }

    // Conecta con Transbank WebPay Plus (ambiente de integración oficial) para generar token y URL
    public TransaccionRespuestaDTO iniciarTransaccionWebpay(String buyOrder, String sessionId, double amount, String returnUrl) {
        try {
            // Código de comercio y API Key públicos de prueba de Transbank SDK 6.x
            WebpayPlus.Transaction tx = WebpayPlus.Transaction.buildForIntegration(
                    "597055555532",
                    IntegrationApiKeys.WEBPAY
            );

            var response = tx.create(buyOrder, sessionId, amount, returnUrl);
            log.info("Token WebPay generado con éxito: {}", response.getToken());
            return new TransaccionRespuestaDTO(response.getToken(), response.getUrl());

        } catch (Exception e) {
            log.error("Error al conectar con Transbank: {}", e.getMessage());
            throw new RuntimeException("Error al iniciar pago en WebPay");
        }
    }

    // Limpia el contador de intentos cuando WebPay confirma (o rechaza) el pago en el callback
    @Transactional
    public void gestionarResultadoCallbackPago(String idCliente, boolean esExitoso) {
        EstadoPagoCliente estado = estadoRepositorio.findById(idCliente).orElse(null);
        if (estado == null) return;

        estado.setCantidadIntentos(0);
        estado.setBloqueado(false);
        estado.setBloqueadoHasta(null);
        estadoRepositorio.save(estado);
        log.info("Callback procesado (exitoso={}) para {}. Contador reiniciado.", esExitoso, idCliente);
    }
}