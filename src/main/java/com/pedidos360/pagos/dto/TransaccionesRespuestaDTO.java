package com.pedidos360.pagos.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransaccionRespuestaDTO {
    private String token;     
    private String urlWebpay; 
}