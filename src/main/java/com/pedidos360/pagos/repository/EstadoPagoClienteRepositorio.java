package com.pedidos360.pagos.repository;

import com.pedidos360.pagos.model.EstadoPagoCliente;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository 
public interface EstadoPagoClienteRepositorio extends JpaRepository<EstadoPagoCliente, String> {
}