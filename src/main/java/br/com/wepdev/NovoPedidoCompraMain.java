package br.com.wepdev;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 *
 */
public class NovoPedidoCompraMain
{
    public static void main( String[] args ) throws ExecutionException, InterruptedException {

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties());

        String key = UUID.randomUUID().toString(); // Simulando o ID de um usuario
        String valor = key + "12123,92313, 5656"; // valores passados pelo produtor

        ProducerRecord<String, String> record = new ProducerRecord("ECOMMERCE_LOJA_NOVO_PEDIDO", key, valor); //Enviando a msg para o tipico ECOMMERCE_LOJA_NOVO_PEDIDO
        Callback callback = (dados, ex) -> { // Ao enviar a msg com o SEND, estou pegando o retorno desse envio
            if (ex != null) { // se acontecer a exception o erro sera exibido
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviando nesse topico " + dados.topic() + ":::particao " + dados.partition() + "/ offset " + dados.offset() + "/ timestamp " + dados.timestamp());
        };
        String email = "Obrigado pelo seu pedido, ele esta sendo processado";
        ProducerRecord<String, String> emailRecord = new ProducerRecord("ECOMMERCE_ENVIAR_EMAIL", key, email);
        producer.send(record, callback).get();
        producer.send(emailRecord, callback).get();
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
}
