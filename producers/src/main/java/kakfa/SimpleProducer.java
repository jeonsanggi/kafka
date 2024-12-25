package kakfa;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer {
    public static void main(String[] args) {

        String topicName = "simple-topic";

        Properties properties = new Properties();

        //bootstrap.server, key.serializer.class, value.serializer.class properties set
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //kafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //ProducerRecord 객체 생성
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "hello world");

        //KafkaProducer message 전송
        kafkaProducer.send(record);

        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
