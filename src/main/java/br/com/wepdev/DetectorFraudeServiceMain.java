package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DetectorFraudeServiceMain {

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties());
            consumer.subscribe(Collections.singletonList("ECOMMERCE_LOJA_NOVO_PEDIDO"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

                if (!records.isEmpty()) {
                    System.out.println("Encontrei " + records.count() +  " registros");
                    for (ConsumerRecord<String, String> record : records) {
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
            }
    }

    /**
     * Metodo de configuracao do consumidor
     * @return
     */
    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Local de configuracao
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Chave serializadora de String
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Valor serializador de String
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, DetectorFraudeServiceMain.class.getSimpleName()); //Criando grupo e colocando o nome da propria classe

        return properties;
    }

}
