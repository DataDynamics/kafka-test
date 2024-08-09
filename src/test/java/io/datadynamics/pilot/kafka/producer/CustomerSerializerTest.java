package io.datadynamics.pilot.kafka.producer;

import io.datadynamics.pilot.kafka.entity.Customer;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class CustomerSerializerTest {
    @Test
    public void testCustomerSerializer(){
        CustomerSerializer serializer = new CustomerSerializer();
        Customer customer = new Customer(1, "John Doe");

        byte[] serializedCustomer = serializer.serialize("kafka-code-test", customer);

        byte[] expectedSerialization = customerSerialize(customer);

        assertNotNull(serializedCustomer);
        assertArrayEquals(expectedSerialization, serializedCustomer);
    }

    public byte[] customerSerialize(Customer data) {
        byte[] serializedName;
        int stringSize;

        serializedName = data.getName().getBytes(StandardCharsets.UTF_8);
        stringSize = serializedName.length;

        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
        buffer.putInt(data.getID());
        buffer.putInt(stringSize);
        buffer.put(serializedName);

        return buffer.array();
    }
}
