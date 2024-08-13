package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.Map;

import java.util.regex.Pattern;

public class LogServiceMain {

    public static void main(String[] args) {

        LogServiceMain logServiceMain = new LogServiceMain();

        try (KafkaService service = new KafkaService(
                LogServiceMain.class.getSimpleName(), // passando o nome do grupo no qual esse consumer pertence
                Pattern.compile("ECOMMERCE.*"), // passando o topico de consumo, todos que tiverem ecommerce na frente
                logServiceMain::parse,
                String.class,
                /*
                Tem momentos que a gente não sabe o que esta vindo no producer, isso e raro de acontecer,
                somente quando não existe um padrão para a mensagem, então e preciso utilizar um deserialized compativel,
                o Map abaixo serve para passar propriedades extras de configuracão do kafka
                 */
                Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {

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
