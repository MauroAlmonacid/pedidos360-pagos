package com.pedidos360.pagos.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class IntentoPagoDTO {
    private String idCliente;      
    private Long monto;           
    private String idTransaccion;  
    private LocalDateTime timestamp; 
}