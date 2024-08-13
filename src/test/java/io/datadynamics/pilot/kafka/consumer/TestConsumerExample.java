package io.datadynamics.pilot.kafka.consumer;

import org.junit.jupiter.api.Test;

import java.util.Properties;

public class TestConsumerExample {

    @Test
    public void testConsumerExample() {
        Consumer consumer = new Consumer();
        Properties kafkaProps = consumer.loadKafkaProps();
        consumer.pullMessages(kafkaProps);
    }

}

