package org.zhangdi.service;

import org.zhangdi.kafka.KafkaProducerFactory;

/**
 * Created by zhangdi on 2017/02/28.
 */
public class DemoMain {

    public static void main(String[] args) {
        KafkaProducerFactory kafkaProducerFactory = new KafkaProducerFactory();


        kafkaProducerFactory.openProducer();

        kafkaProducerFactory.producer("topicDemo","keyDemo","this line is demo");


        kafkaProducerFactory.closeProducer();
    }
}
