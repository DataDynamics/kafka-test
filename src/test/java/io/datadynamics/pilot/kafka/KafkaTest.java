package io.datadynamics.pilot.kafka;

import java.util.UUID;

public class KafkaTest {

    public static void main(String[] args) {
        PlatformProducer p = new PlatformProducer(
                "Impala",
                UUID.randomUUID().toString(),
                "io.datadynamics.pilot.kafka.PlatformCallbackImpl",
                "http://10.0.0.1:8080"
        );

        p.send(new KafkaRecord("A", "JSON"));

    }

}
