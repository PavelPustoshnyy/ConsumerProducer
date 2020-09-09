import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    String bootstrapServers;
    String groupId;
    String topic;
    Logger logger;
    Properties properties;

    public ConsumerDemo(String bootstrapServers, String groupId, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topic = topic;
        this.logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        // create consumer configs
        this.properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Collections.singleton(topic));

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records
                    = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
                logger.info(record.value());
            }
        }

    }
}
