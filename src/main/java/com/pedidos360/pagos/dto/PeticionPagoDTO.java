package com.pedidos360.pagos.dto;

import lombok.Data;

@Data
public class PeticionPagoDTO {
    private String clientId;    
    private Long amount;          
    private String transactionId; 
}