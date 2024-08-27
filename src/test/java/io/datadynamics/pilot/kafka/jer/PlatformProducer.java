package io.datadynamics.pilot.kafka.jer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class PlatformProducer {
    private final String systemName;
    private final String producerId;
    private final String url;
    private Properties props;

    private KafkaProducer<String, String> producer;

    public PlatformProducer(String systemName, String producerId, String url) {
        this.systemName = systemName;
        this.producerId = producerId;
        this.url = url;

        this.props = getProperties(url);

        this.producer = new KafkaProducer<>(props);
    }

    private Properties getProperties(String url) {
        // url로 properties JSON(string) 가져오기
        String jsonString = "{\n" +
                "  \"logging-api\" : \"/api/v1/config\",\n" +
                "  \"producer\": {\n" +
                "    \"boostrap-servers\": [\n" +
                "      \"server1\",\n" +
                "      \"server2\",\n" +
                "      \"server3\"\n" +
                "    ],\n" +
                "    \"key-serializer\": \"StringSerializer\",\n" +
                "    \"value-serializer\": \"StringSerializer\",\n" +
                "    \"acks\": \"all\",\n" +
                "    \"enable-idempotence\": false\n" +
                "  }\n" +
                "}";

        // string to JSON || map

        return addToProperties();
    }

    private Properties addToProperties() {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.0.1.67:9092,10.0.1.68:9092,10.0.1.69:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);

        return properties;
    }

    public void close() {
        producer.close();
    }

    public Future<RecordMetadata> send(KafkaRecord record) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(record.topic(), record.key(), record.value());
        producerRecord.headers().add("system-name", systemName.getBytes());
        producerRecord.headers().add("producer-id", producerId.getBytes());
        return producer.send(producerRecord);
    }
}
