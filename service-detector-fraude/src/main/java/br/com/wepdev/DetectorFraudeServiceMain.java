package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Map;


public class DetectorFraudeServiceMain {

    public static void main(String[] args) {

        DetectorFraudeServiceMain detectorFraudeServiceMain = new DetectorFraudeServiceMain();

        try(KafkaService<Compra> kafkaService = new KafkaService<>(
                DetectorFraudeServiceMain.class.getSimpleName(), // passando o nome do grupo no qual esse consumer pertence
                "ECOMMERCE_NOVO_PEDIDO", // passando o topico de consumo
                detectorFraudeServiceMain::parse,
                Compra.class,
                Map.of())) { // Passando um Map.of() vazio, pois esse service não recebe configuracoes de properties customizadas

            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, Compra> record) {
        System.out.println("--------------------------------------------------------------------");
        System.out.println("Processando novos pedidos, checando fraudes ");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Pedido processado com sucesso!!!");
    }


}
