package io.datadynamics.pilot.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.UUID;

public class KafkaRecord {

    ProducerRecord<String, String> record;

    String topic;
    String key = UUID.randomUUID().toString();
    String value;

    public KafkaRecord(String topic, String value) {
        this.topic = topic;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public KafkaRecord setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public String getValue() {
        return value;
    }

    public KafkaRecord setValue(String value) {
        this.value = value;
        return this;
    }

    public String getKey() {
        return key;
    }
}
