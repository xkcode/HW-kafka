package com.xkcode.hw.kafka.v1.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.xkcode.hw.kafka.v1.producer.KafkaProducer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class KafkaConsumer {

    private final ConsumerConnector consumer;

    private KafkaConsumer() {
        Properties props = new Properties();
        //zookeeper 配置
        props.put("zookeeper.connect", "10.88.1.162:4180");

        //group 代表一个消费组
        props.put("group.id", "jd-group");

        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(props);

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    void consume() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(KafkaProducer.TOPIC, new Integer(1));
        System.out.println("----------consumer topicCountMap initialed----------");
        
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = 
                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        System.out.println("----------consumer stream initialed----------");
        KafkaStream<String, String> stream = consumerMap.get(KafkaProducer.TOPIC).get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        System.out.println("----------consumer iterator retrieved----------");
        while (it.hasNext())
            System.out.println(it.next().message());
    }

    public static void main(String[] args) {
        new KafkaConsumer().consume();
    }
}