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

        String jsonValue = "{\n" +
                "  \"meesageId\":" + UUID.randomUUID() + ",\n" +
                "  \"id\":\"testId\",\n" +
                "  \"datalakeName\":\"Impala\",\n" +
                "  \"dataPath:\":\"/path\",\n" +
                "  \"fileName\":\"filename\",\n" +
                "  \"fullPath\":\"/fullPath/to/datafile\",\n" +
                "  \"filesize\":1,\n" +
                "  \"time\":\"2024-08-28 16:22:33\",\n" +
                "  \"rows\":100,\n" +
                "  \"host\":\"hostname\",\n" +
                "  \"createDateTs\":\"2024-08-28 16:22:33\",\n" +
                "  \"updateDateTs\":null\n" +
                "}";

        producer.send(new KafkaRecord("all-messages", jsonValue));
        producer.close();
    }
}
