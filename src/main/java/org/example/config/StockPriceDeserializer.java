package org.example.config;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.example.beans.StockPrice;
import tools.jackson.databind.ObjectMapper;

public class StockPriceDeserializer implements Deserializer<StockPrice> {
    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public StockPrice deserialize(String topic, byte[] data) {
        return mapper.readValue(data, StockPrice.class);
    }
}
