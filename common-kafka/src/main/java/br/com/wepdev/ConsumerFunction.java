package br.com.wepdev;

import org.apache.kafka.clients.consumer.ConsumerRecord;


public interface ConsumerFunction<T> {

    void consume(ConsumerRecord<String, T> record);
}
