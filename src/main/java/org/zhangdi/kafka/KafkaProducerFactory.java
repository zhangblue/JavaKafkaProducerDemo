package org.zhangdi.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Created by zhangdi on 2017/02/28.
 * <p>
 * kafka生产者
 */
public class KafkaProducerFactory {
    private Producer<String, String> producer = null;

    /***
     * 打开kafka连接
     */
    public void openProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.101.185.156:9092,10.101.185.152:9092");//kafka服务器地址
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer(props);

    }


    /***
     * kafka生产者
     * @param topic topic名字
     * @param key 可重复
     * @param line 实际数据
     */
    public void producer(String topic, String key, String line) {
        producer.send(new ProducerRecord<String, String>(topic, key, line), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //System.out.println("本次产生了[" + recordMetadata.offset() + "]条数据");
            }
        });
    }


    /***
     * 关闭连接
     */
    public void closeProducer() {
        if (producer != null) {
            producer.close();
        }
    }
}
