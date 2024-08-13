package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

 class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    /**
     * Construtor
     * @param nomeGrupoID passa no properties o grupo no qual esse consumer pertence
     * @param topic  passa o topico que sera consumido
     * @param parse funcao parse
     */
    public KafkaService(String nomeGrupoID, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties){
        this(parse, nomeGrupoID, type, properties); // chamando o construtor privado de baixo
        consumer.subscribe(Collections.singletonList(topic));
    }

    /**
     * Contrutor 2 que recebe um pattern para atender ao logservicemain
     * @param nomeGrupoID
     * @param topic
     * @param parse
     */
    public KafkaService(String nomeGrupoID, Pattern topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties){
        this(parse, nomeGrupoID, type, properties); // chamando o construtor privado de baixo
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction parse, String nomeGrupoID, Class<T> type, Map<String, String> properties){
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties( type, nomeGrupoID, properties));

    }

    public void run() {
        while (true) {
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(200));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() +  " registros");
                for (ConsumerRecord<String, T> record : records) {
                    parse.consume(record);
                }
            }
        }
    }

    /**
     * Metodo de configuracao do consumidor
     * @return
     */
    private Properties getProperties(Class<T> type, String nomeGrupoID, Map<String, String> overrideProperties) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Local de configuracao
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // Chave serializadora de String
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserialized.class.getName()); // Valor serializador de String
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, nomeGrupoID); //Criando grupo e recebendo um nome para ele
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()); //Criando um ID generico
        properties.setProperty(GsonDeserialized.TYPE_CONFIG, type.getName()); // configuracao de propriedade que deserializa qualquer tipo de classe
        properties.putAll(overrideProperties);

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
