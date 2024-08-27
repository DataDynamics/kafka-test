package io.datadynamics.pilot.kafka.jer;

import org.junit.jupiter.api.Test;

import java.util.UUID;

public class KafkaTest {
    @Test
    public void platformCustomTest() {
        PlatformProducer producer = new PlatformProducer(
                "Impala",
                UUID.randomUUID().toString(),
                "https://jsonplaceholder.typicode.com/posts/1"
        );

        producer.send(new KafkaRecord("all-messages", "json"));
        producer.close();
    }
}
