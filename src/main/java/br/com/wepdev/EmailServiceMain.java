package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailServiceMain {

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties());
            consumer.subscribe(Collections.singletonList("ECOMMERCE_ENVIAR_EMAIL"));// Esse consumer , consome os dados do topico ECOMMERCE_ENVIAR_EMAIL

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

                if (!records.isEmpty()) {
                    System.out.println("Encontrei " + records.count() +  " registros");
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("--------------------------------------------------------------------");
                        System.out.println("Enviando email ");
                        System.out.println(record.key());
                        System.out.println(record.value());
                        System.out.println(record.partition());
                        System.out.println(record.offset());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        System.out.println("Email foi enviado!!!");
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
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, EmailServiceMain.class.getSimpleName()); //Criando grupo e colocando o nome da propria classe

        return properties;
    }

}
