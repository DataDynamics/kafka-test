package io.datadynamics.pilot.kafka.jer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class PlatformProducer {
    private final String systemName;
    private final String producerId;
    private final String url;
    private Properties props;
    private String loggingApi;

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
                "    \"bootstrap-servers\": [\n" +
                "      \"10.0.1.67:9092\",\n" +
                "      \"10.0.1.68:9092\",\n" +
                "      \"10.0.1.69:9092\"\n" +
                "    ],\n" +
                "    \"key-serializer\": \"org.apache.kafka.common.serialization.StringSerializer\",\n" +
                "    \"value-serializer\": \"org.apache.kafka.common.serialization.StringSerializer\",\n" +
                "    \"acks\": \"all\",\n" +
                "    \"enable-idempotence\": false\n" +
                "  }\n" +
                "}";

        // string to JSON
        Properties props = new Properties();
        ObjectMapper mapper = new ObjectMapper();

        try {
            JsonNode node = mapper.readTree(jsonString);

            props = addToProperties(props, node, "");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return props;
    }

    private Properties addToProperties(Properties props, JsonNode node, String key) {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                if ("logging-api".equals(field.getKey())) {
                    this.loggingApi = field.getValue().textValue();
                } else {
                    addToProperties(props, field.getValue(), field.getKey().replace("-", "."));
                }
            }
        } else if (node.isArray()) {
            StringBuilder value = new StringBuilder();
            for (JsonNode element : node) {
                value.append(element.asText()).append(",");
            }
            if (value.length() > 0) {
                value.setLength(value.length() - 1); // 마지막 쉼표 제거
            }
            props.setProperty(key, value.toString());
        } else {
            props.setProperty(key, node.asText());
        }

        return props;
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
