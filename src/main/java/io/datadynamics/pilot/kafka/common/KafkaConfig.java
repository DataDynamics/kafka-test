package io.datadynamics.pilot.kafka.common;

import io.datadynamics.pilot.kafka.producer.Producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaConfig {
    public static Properties loadKafkaProps() {
        Properties properties = new Properties();
        try (InputStream input = Producer.class.getClassLoader().getResourceAsStream("kafka.properties")) {
            if (input == null) {
                System.out.println("No kafka properties found");
                return properties;
            }
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
