package com.pedidos360.pagos.controller;

import cl.transbank.common.IntegrationApiKeys;
import cl.transbank.webpay.webpayplus.WebpayPlus;
import com.pedidos360.pagos.dto.PeticionPagoDTO;
import com.pedidos360.pagos.dto.TransaccionRespuestaDTO;
import com.pedidos360.pagos.service.PagosService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/pagos")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class PagosController {

    private final PagosService pagosService;

    @Value("${webpay.return-url}")
    private String webpayReturnUrl;

    @PostMapping("/iniciar")
    public ResponseEntity<?> iniciarPago(@RequestBody PeticionPagoDTO peticion) {
        // 1. Validar que vengan datos mínimos
        if (peticion.getClientId() == null || peticion.getAmount() == null || peticion.getAmount() <= 0) {
            return ResponseEntity.badRequest().body(Map.of("message", "clientId y amount (>0) son obligatorios."));
        }

        // 2. Verificar en H2 si el consumidor de Kafka bloqueó a este cliente por fraude
        if (pagosService.verificarSiUsuarioEstaBloqueado(peticion.getClientId())) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(Map.of("message", "Demasiados intentos detectados. Estás bloqueado temporalmente por 30 minutos."));
        }

        // 3. Publicar intento en Kafka para auditoría de velocidad (asíncrono)
        String txId = peticion.getTransactionId() != null ? peticion.getTransactionId() : "TX-" + System.currentTimeMillis();
        pagosService.producirIntentoPago(peticion.getClientId(), peticion.getAmount(), txId);

        // 4. Crear transacción real en Transbank WebPay Plus (ambiente integración)
        try {
            TransaccionRespuestaDTO transaccion = pagosService.iniciarTransaccionWebpay(
                    txId,
                    peticion.getClientId(),
                    peticion.getAmount().doubleValue(),
                    webpayReturnUrl
            );
            return ResponseEntity.ok(Map.of(
                    "url", transaccion.getUrlWebpay(),
                    "token", transaccion.getToken()
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "No se pudo conectar con la pasarela WebPay Plus"));
        }
    }

    @PostMapping("/commit")
    public ResponseEntity<?> confirmarPagoWebpay(@RequestParam("token_ws") String tokenWs) {
        try {
            WebpayPlus.Transaction tx = WebpayPlus.Transaction.buildForIntegration(
                    "597055555532",
                    IntegrationApiKeys.WEBPAY
            );
            var response = tx.commit(tokenWs);

            if ("AUTHORIZED".equals(response.getStatus())) {
                log.info("Pago APROBADO en WebPay. Orden: {}, Monto: {}", response.getBuyOrder(), response.getAmount());
                pagosService.gestionarResultadoCallbackPago(response.getSessionId(), true);
                return ResponseEntity.ok(response);
            } else {
                log.warn("Pago RECHAZADO en WebPay. Orden: {}", response.getBuyOrder());
                pagosService.gestionarResultadoCallbackPago(response.getSessionId(), false);
                return ResponseEntity.badRequest().body(Map.of("message", "El pago fue rechazado por Transbank"));
            }
        } catch (Exception e) {
            log.error("Error al confirmar token_ws en Transbank: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "Error al confirmar la transacción: " + e.getMessage()));
        }
    }
}