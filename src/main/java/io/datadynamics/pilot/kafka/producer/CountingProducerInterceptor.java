package io.datadynamics.pilot.kafka.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CountingProducerInterceptor implements ProducerInterceptor {
    static AtomicInteger numSent = new AtomicInteger(0);
    static AtomicInteger numAcked = new AtomicInteger(0);
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        System.out.println("numSent: " + numSent.incrementAndGet());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        System.out.println("numAcked: " + numAcked.incrementAndGet());
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}