package io.datadynamics.pilot.kafka.consumer;

import io.datadynamics.pilot.kafka.common.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public Properties loadKafkaProps() {
        Properties kafkaProps = KafkaConfig.loadKafkaProps();
        kafkaProps.put("group.id", "KafkaCodeTest");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return kafkaProps;
    }
    public void pullMessages(Properties kafkaProps) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProps);
        consumer.subscribe(Collections.singletonList("kafka-code-test"));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value()
            );
        }
        consumer.close();
    }

}
