package org.example.config;


import org.apache.kafka.common.serialization.Serializer;
import org.example.beans.StockSignal;
import tools.jackson.databind.ObjectMapper;

public class StockSignalSerializer implements Serializer<StockSignal> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, StockSignal data) {
        return mapper.writeValueAsBytes(data);
    }
}
