package org.example.config;

import org.apache.kafka.common.serialization.Deserializer;
import org.example.beans.StockSignal;
import tools.jackson.databind.ObjectMapper;

public class StockSignalDeserializer implements Deserializer<StockSignal> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public StockSignal deserialize(String topic, byte[] data) {
        return mapper.readValue(data, StockSignal.class);
    }
}
