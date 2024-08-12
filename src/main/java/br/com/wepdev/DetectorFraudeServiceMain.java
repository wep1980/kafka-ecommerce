package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public class DetectorFraudeServiceMain {

    public static void main(String[] args) {

        DetectorFraudeServiceMain detectorFraudeServiceMain = new DetectorFraudeServiceMain();

        try(KafkaService kafkaService = new KafkaService(
                DetectorFraudeServiceMain.class.getSimpleName(), // passando o nome do grupo no qual esse consumer pertence
                "ECOMMERCE_LOJA_NOVO_PEDIDO", // passando o topico de consumo
                detectorFraudeServiceMain::parse)) { // (Method reference -> passando uma referencia para a função) Função executada para cada mensagem recebida

            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
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
