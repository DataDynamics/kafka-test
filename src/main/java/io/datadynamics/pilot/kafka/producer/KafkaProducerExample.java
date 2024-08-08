package io.datadynamics.pilot.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerExample {
        private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        Properties kafkaProps = loadKafkaProps();
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = createProducer(kafkaProps);

        try {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("kafka-code-test",
                            "empty key, fire-and-forget, from .props, value 1");

//            sendMessageSync(record);
//            sendMessageAsync(record);
            sendMessage(record);
//            sendMessageWithHeader(record);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            // close()를 안 하면 메시지 손실이 거의 90퍼 발생
            closeProducer();
        }
    }

    public static void sendMessageWithHeader(ProducerRecord<String, String> record) {
        record.headers().add("privacy-level","YOLO".getBytes(StandardCharsets.UTF_8));
        producer.send(record);
    }

    public static void sendMessage(ProducerRecord<String, String> record) throws Exception {
        // fire-and-forget 전송 방식
        producer.send(record);
    }

    public static void sendMessageAsync(ProducerRecord<String, String> record) throws Exception {
        producer.send(record,
                new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        System.out.printf("Message sent to topic: %s, partition: %s, offset: %d%n",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    }
        });
    }

    public static void sendMessageSync(ProducerRecord<String, String> record) throws Exception {
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();
        System.out.printf("Message sent to topic: %s, partition: %s, offset: %d%n",
                metadata.topic(), metadata.partition(), metadata.offset());
    }

    private static KafkaProducer<String, String> createProducer(Properties kafkaProps) {
        return new KafkaProducer<>(kafkaProps);
    }

    private static void closeProducer() {
        if(producer != null)
            producer.close();
    }

    public static Properties loadKafkaProps() {
        Properties properties = new Properties();
        try (InputStream input = KafkaProducerExample.class.getClassLoader().getResourceAsStream("kafka.properties")) {
            if (input == null) {
                System.out.println("No kafka properties found");
                return properties;
            }
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
