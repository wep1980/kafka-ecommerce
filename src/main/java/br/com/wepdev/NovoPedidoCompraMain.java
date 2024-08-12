package br.com.wepdev;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 *
 */
public class NovoPedidoCompraMain {
    public static void main( String[] args ) throws ExecutionException, InterruptedException {

        try(KafkaDispatcher compraDispatcher = new KafkaDispatcher<Compra>()) { // qualquer exception que aconteça aqui dentro, existira uma tentativa de fechar esse producer
            try (KafkaDispatcher emailDispatcher = new KafkaDispatcher<String>()) {


                for (int i = 0; i < 10; i++) {

                    String usuarioId = UUID.randomUUID().toString(); // Simulando o ID de um usuario
                    String compraId = UUID.randomUUID().toString(); // Simulando o ID de um usuario
                    BigDecimal valorTotal = new BigDecimal(Math.random() * 5000 + 1); // Transformando um valor de retorno double em bigDecimal

                    Compra compra = new Compra(usuarioId, compraId, valorTotal);

                    compraDispatcher.send("ECOMMERCE_LOJA_NOVO_PEDIDO", usuarioId, compra); // enviando mensagem para o topico ECOMMERCE_LOJA_NOVO_PEDIDO

                    String email = "Obrigado pelo seu pedido, ele esta sendo processado";
                    emailDispatcher.send("ECOMMERCE_ENVIAR_EMAIL", usuarioId, email); // Enviando mensagem para o topico ECOMMERCE_ENVIAR_EMAIL
                }
            }
        }
    }
}



