package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;


public class DetectorFraudeServiceMain {

    public static void main(String[] args) {

        DetectorFraudeServiceMain detectorFraudeServiceMain = new DetectorFraudeServiceMain();

        KafkaService kafkaService = new KafkaService(
                DetectorFraudeServiceMain.class.getSimpleName(),
                "ECOMMERCE_LOJA_NOVO_PEDIDO",
                detectorFraudeServiceMain::parse);

        kafkaService.run();

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




    /**
     * Metodo de configuracao do consumidor
     * @return
     */
    private static Properties properties() {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, DetectorFraudeServiceMain.class.getSimpleName());

        return properties;
    }

}
