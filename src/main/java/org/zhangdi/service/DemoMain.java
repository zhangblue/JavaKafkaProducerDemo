package org.zhangdi.service;

import java.util.List;
import org.zhangdi.guava.GuavaFilesTools;
import org.zhangdi.kafka.KafkaProducerFactory;

/**
 * Created by zhangdi on 2017/02/28.
 */
public class DemoMain {

    public static void main(String[] args) {
        KafkaProducerFactory kafkaProducerFactory = new KafkaProducerFactory();
        kafkaProducerFactory.openProducer();


        List<String> list = new GuavaFilesTools().readSmallFile("/Users/zhangdi/work/workspace/nanhang/json_format.log");




        //String jsonDemo = "{\"remote_address\":\"1.1.1.9\",\"user_agent\":\"Mozilla/5.0 (iPhone; CPU iPhone OS 10_1 like Mac OS X; zh-CN) \",\"cookie\":\"1c4aebfb-8aa4-43b4-b4af-5890646ae742\",\"url\":\"/CSMBP/data/account/login/loginQuickPwd.do\",\"rx_time\":\"2017-08-31T17:18:31+8\"}";

        System.out.println("begin = " + System.nanoTime());

        for (String line:list) {
            kafkaProducerFactory.producer("nanhang-topic2", "nanhang-key", line);
        }
        System.out.println("end = " + System.nanoTime());
        kafkaProducerFactory.closeProducer();
    }
}


