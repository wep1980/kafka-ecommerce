package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailServiceMain {

    public static void main(String[] args) {

        EmailServiceMain emailServiceMain = new EmailServiceMain();

        KafkaService service = new KafkaService(
                "ECOMMERCE_ENVIAR_EMAIL",
                "ECOMMERCE_LOJA_NOVO_PEDIDO",
                emailServiceMain::parse);

        service.run();

    }
        private void parse (ConsumerRecord<String,String > record){
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
