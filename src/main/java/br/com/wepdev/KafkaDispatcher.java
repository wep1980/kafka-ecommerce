package br.com.wepdev;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher implements Closeable {


    private final KafkaProducer<String, String> producer;

    /**
     * Construtor
     */
    public KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }


    public void send(String topic, String key, String valor) throws ExecutionException, InterruptedException {

        ProducerRecord<String, String> record = new ProducerRecord(topic, key, valor); //Enviando a msg para o tipico ECOMMERCE_LOJA_NOVO_PEDIDO
        Callback callback = (dados, ex) -> { // Ao enviar a msg com o SEND, estou pegando o retorno desse envio
            if (ex != null) { // se acontecer a exception o erro sera exibido
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviando nesse topico " + dados.topic() + ":::particao " + dados.partition() + "/ offset " + dados.offset() + "/ timestamp " + dados.timestamp());
        };
        producer.send(record, callback).get();
    }

    /**
     * Metodo de configuracao das propriedades do Produtor
     * @return
     */
    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Local de configuracao
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Chave serializadora de String
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // Valor serializador de String

        return properties;
    }

    /**
     * Com esse metodo de close explicito, e possivel usar as caracteristicas do java de tentar executar esse codigo,
     * utilizando try catch no EmailServiceMain e no DetectorFraudeServiceMain
     */
    @Override
    public void close() {
        producer.close();
    }
}
