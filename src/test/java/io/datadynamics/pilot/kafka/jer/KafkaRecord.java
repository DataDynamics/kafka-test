package io.datadynamics.pilot.kafka.jer;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.UUID;

public class KafkaRecord {
    private final String topic;
    private final String value;
    private final String key = UUID.randomUUID().toString();

    ProducerRecord<String, String> record;

    public KafkaRecord(String topic, String value) {
        this.topic = topic;
        this.value = value;
    }

    public String topic() {
        return topic;
    }

    public String key() {
        return key;
    }

    public String value() {
        return value;
    }
}
