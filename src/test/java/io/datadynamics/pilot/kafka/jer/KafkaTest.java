package io.datadynamics.pilot.kafka.jer;

import org.junit.jupiter.api.Test;

import java.util.UUID;

public class KafkaTest {
    @Test
    public void platformCustomTest() {
        String producerId = UUID.randomUUID().toString();
        PlatformProducer producer = new PlatformProducer(
                "Impala",
                producerId,
                "http://10.0.0.1:8080/kafka/producer/options/" + producerId
        );

        producer.send(new KafkaRecord("all-messages", "json"));
        producer.close();
    }
}
