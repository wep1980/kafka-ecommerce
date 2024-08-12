package br.com.wepdev;

import java.math.BigDecimal;

public class Compra {

    private final String usuarioId, compraId;
    private final BigDecimal valorTotal;


    public Compra(String usuarioId, String compraId, BigDecimal valorTotal) {
        this.usuarioId = usuarioId;
        this.compraId = compraId;
        this.valorTotal = valorTotal;
    }
}
