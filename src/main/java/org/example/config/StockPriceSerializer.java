package org.example.config;

import org.apache.kafka.common.serialization.Serializer;
import org.example.beans.StockPrice;
import tools.jackson.databind.ObjectMapper;

public class StockPriceSerializer implements Serializer<StockPrice> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, StockPrice data) {
        return mapper.writeValueAsBytes(data);
    }
}
