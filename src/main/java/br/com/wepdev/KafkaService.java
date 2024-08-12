package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaService implements Closeable {

    private final KafkaConsumer consumer;

    private final ConsumerFunction parse;

    /**
     * Construtor
     * @param nomeGrupoID passa no properties o grupo no qual esse consumer pertence
     * @param topic  passa o topico que sera consumido
     * @param parse funcao parse
     */
    public KafkaService(String nomeGrupoID, String topic, ConsumerFunction parse){
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(nomeGrupoID));
        consumer.subscribe(Collections.singletonList(topic));


    }

    public void run() {
        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() +  " registros");
                for (ConsumerRecord<String, String> record : records) {
                    parse.consume(record);
                }
            }
        }
    }

    /**
     * Metodo de configuracao do consumidor
     * @return
     */
    private static Properties properties(String nomeGrupoID) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Local de configuracao
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Chave serializadora de String
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Valor serializador de String
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, nomeGrupoID); //Criando grupo e recebendo um nome para ele
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()); //Criando um ID generico

        return properties;
    }

    /**
     * Com esse metodo de close explicito, e possivel usar as caracteristicas do java de tentar executar esse codigo,
     * utilizando try catch no EmailServiceMain e no DetectorFraudeServiceMain
     */
    @Override
    public void close() {
        consumer.close();
    }
}
