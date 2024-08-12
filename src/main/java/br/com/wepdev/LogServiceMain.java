package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogServiceMain {

    public static void main(String[] args) {

        LogServiceMain logServiceMain = new LogServiceMain();

        try (KafkaService service = new KafkaService(
                LogServiceMain.class.getSimpleName(), // passando o nome do grupo no qual esse consumer pertence
                Pattern.compile("ECOMMERCE.*"), // passando o topico de consumo, todos que tiverem ecommerce na frente
                logServiceMain::parse,
                String.class)) { // (Method reference -> passando uma referencia para a função) Função executada para cada mensagem recebida

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {

        System.out.println("--------------------------------------------------------------------");
        System.out.println("LOG: " + record.topic());
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

    }


}
