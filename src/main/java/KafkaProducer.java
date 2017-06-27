import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;



public class KafkaProducer {
    private final Producer<String, String> producer;
    public final static String TOPIC = "test";
    private final Logger logger= LoggerFactory.getLogger(KafkaProducer.class);

    public KafkaProducer() {
        Properties props = new Properties();
        // 此处配置的是kafka的端口
        props.put("metadata.broker.list", "192.168.6.233:9092,192.168.6.234:9092,192.168.6.237:9092");

        // 配置value的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // 配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "-1");

        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    void produce(String send_data) {
        String key = send_data;
        String data = "hello kafka message: " + key;
        producer.send(new KeyedMessage<String, String>(TOPIC, key, data));
        logger.info("这里是一条info日志信息");
        logger.warn("这里是一条warn日志信息");
        //System.out.println(data);
    }

    public String test(){
        return "seccess";
    }

    public static void main(String[] args) {
        new KafkaProducer().produce("500");
    }
}