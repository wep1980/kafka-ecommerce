package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailServiceMain {

    public static void main(String[] args) {

        EmailServiceMain emailServiceMain = new EmailServiceMain();

       try(KafkaService service = new KafkaService(
                EmailServiceMain.class.getSimpleName(), // passando o nome do grupo no qual esse consumer pertence
                "ECOMMERCE_LOJA_NOVO_PEDIDO", // passando o topico de consumo
                emailServiceMain::parse,
                String.class)) { // (Method reference -> passando uma referencia para a função) Função executada para cada mensagem recebida

           service.run();
       }

    }

        /**
         * Funcao executada para cada mensagem recebida.
         * Esse metodo poderia ser static, ao inves de uma funcao
         * @param record
         */
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
