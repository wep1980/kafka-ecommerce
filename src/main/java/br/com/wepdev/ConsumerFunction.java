package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface ConsumerFunction {

    void consume(ConsumerRecord<String, String> record);
}
