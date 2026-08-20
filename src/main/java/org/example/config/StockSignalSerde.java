package org.example.config;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.example.beans.StockSignal;

public class StockSignalSerde implements Serde<StockSignal> {
    @Override
    public Serializer<StockSignal> serializer() {
        return new StockSignalSerializer();
    }

    @Override
    public Deserializer<StockSignal> deserializer() {
        return new StockSignalDeserializer();
    }
}
