package io.datadynamics.pilot.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.Future;

public class PlatformProducer {

    private final String systemName;
    private final String producerId;
    private final String url;
    private Properties props;

    private KafkaProducer<String, String> producer;

    public PlatformProducer(String systemName, String producerId, String callbackClassName, String url) {
        this.systemName = systemName;
        this.producerId = producerId;
        this.url = url;
        this.props = getProperties(url);

        this.props.setProperty("", callbackClassName); // Interceptor
        this.producer = new KafkaProducer<>(props);
    }

    private Properties getProperties(String url) {
        // url을 호출해서 zookeeper 및 관련 정보를 가져와서 properties에 추가
        return null;
    }

    public Future<RecordMetadata> send(KafkaRecord r) {
        ProducerRecord<String, String> record = new ProducerRecord<>(r.getTopic(), r.getKey(), r.getValue());
        record.headers().add("system-name", systemName.getBytes());
        record.headers().add("producer-id", producerId.getBytes());
        return producer.send(record);
    }

    public void close() {
        producer.close();
    }

    public void close(Duration timeout) {
        producer.close(timeout);
    }
}
