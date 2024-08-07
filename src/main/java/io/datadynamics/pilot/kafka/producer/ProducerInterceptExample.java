package io.datadynamics.pilot.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerInterceptExample {
    public static void main(String[] args) {
        KafkaProducer<String, String> producer = getStringStringKafkaProducer();

        try {
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("kafka-code-test",
                                "empty key, async, for, value " + i);
                producer.send(record);
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static KafkaProducer<String, String> getStringStringKafkaProducer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "10.0.1.67:9092,10.0.1.68:9092,10.0.1.69:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.datadynamics.pilot.kafka.producer.CountingProducerInterceptor");

        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);
        return producer;
    }
}
