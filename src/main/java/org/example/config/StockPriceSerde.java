package org.example.config;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.example.beans.StockPrice;

public class StockPriceSerde implements Serde<StockPrice> {
    @Override
    public Serializer<StockPrice> serializer() {
        return new StockPriceSerializer();
    }

    @Override
    public Deserializer<StockPrice> deserializer() {
        return new StockPriceDeserializer();
    }
}
