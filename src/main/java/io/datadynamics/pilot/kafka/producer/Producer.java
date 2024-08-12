package io.datadynamics.pilot.kafka.producer;

import io.datadynamics.pilot.kafka.common.KafkaConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.Future;

public class Producer {
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        Properties kafkaProps = KafkaConfig.loadKafkaProps();
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        kafkaProps.put("partitioner.class", "io.datadynamics.pilot.kafka.producer.BananaPartitioner");

        producer = createProducer(kafkaProps);

        try {
            sendRepeatedMessages(3);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            closeProducer();
        }
    }

    public static void sendRepeatedMessages(int num) throws Exception {
        for (int i = 0; i < num; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("kafka-code-test",
                            "key null, fire-and-forget, default-partitioner, value " + i);
            chooseMessageSendMode("", record);
        }
    }

    public static void chooseMessageSendMode(String mode, ProducerRecord<String, String> record) throws Exception {
        switch (mode) {
            case "async":
                sendMessageAsync(record);
                break;
            case "sync":
                sendMessageSync(record);
                break;
            case "header":
                sendMessageWithHeader(record);
                break;
            case "fire-and-forget":
            default:
                sendMessage(record);
                break;
        }
    }

    public static void sendMessageWithHeader(ProducerRecord<String, String> record) {
        record.headers().add("privacy-level","YOLO".getBytes(StandardCharsets.UTF_8));
        producer.send(record);
    }

    public static void sendMessage(ProducerRecord<String, String> record) throws Exception {
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

}
