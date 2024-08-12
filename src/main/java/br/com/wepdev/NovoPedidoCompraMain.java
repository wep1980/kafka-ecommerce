package br.com.wepdev;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 *
 */
public class NovoPedidoCompraMain
{
    public static void main( String[] args ) throws ExecutionException, InterruptedException {

        try(KafkaDispatcher dispatcher = new KafkaDispatcher()) { // qualquer exception que aconteça aqui dentro, existira uma tentativa de fechar esse producer

            for (int i = 0; i < 10; i++) {

                String key = UUID.randomUUID().toString(); // Simulando o ID de um usuario
                String valor = key + "12123,92313, 5656"; // valores passados pelo produtor

                dispatcher.send("ECOMMERCE_LOJA_NOVO_PEDIDO", key, valor); // enviando mensagem para o topico ECOMMERCE_LOJA_NOVO_PEDIDO


                String email = "Obrigado pelo seu pedido, ele esta sendo processado";
                dispatcher.send("ECOMMERCE_ENVIAR_EMAIL", key, email); // Enviando mensagem para o topico ECOMMERCE_ENVIAR_EMAIL
            }
        }
    }


}
