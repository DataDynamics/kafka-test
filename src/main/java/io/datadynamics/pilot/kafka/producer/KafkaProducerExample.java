package io.datadynamics.pilot.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerExample {
        private static KafkaProducer producer;

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "10.0.1.67:9092,10.0.1.68:9092,10.0.1.69:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(kafkaProps);

        try {
//            sendMessageSync("kafka-code-test", "", "no key sync value 1");
//            sendMessageAsync("kafka-code-test", "", "no key async value 1");
            sendMessage("kafka-code-test", "no key fire-and-forget value 6");
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            // close()를 안 하면 메시지 손실이 거의 90퍼 발생
            producer.close();
        }
    }

    public static void sendMessage(String topic, String value) throws Exception {
        // fire-and-forget 전송 방식
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
        producer.send(record);
    }

    public static void sendMessageAsync(String topic, String key, String value) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record,
                new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        System.out.printf("Message sent to topic: %s, partition: %s, offset: %d%n",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    }
        });
    }

    public static void sendMessageSync(String topic, String key, String value) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();
        System.out.printf("Message sent to topic: %s, partition: %s, offset: %d%n",
                metadata.topic(), metadata.partition(), metadata.offset());
    }
}
