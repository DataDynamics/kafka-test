package io.datadynamics.pilot.kafka;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public abstract class PlatformCallback implements ProducerInterceptor {

    String uuid;

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        this.uuid = (String) record.key();
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        this.onCompletion(this.uuid, exception);
    }

    @Override
    public void close() {
        // nothing
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // ???
    }

    abstract void onCompletion(String messageId, Exception exception);

}
