package kakfa;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducerAsync {
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsync.class.getName());
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
        kafkaProducer.send(record, (metadata, e) -> {
            if (e == null) {
                logger.info("\n ###### record metadata received ##### \n" +
                    "partition" + metadata.partition() + "\n" +
                    "offset" + metadata.offset() + "\n" +
                    "timestamp" + metadata.timestamp()
                );
            } else {
                logger.error("Exception error from broker " + e.getMessage());
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
