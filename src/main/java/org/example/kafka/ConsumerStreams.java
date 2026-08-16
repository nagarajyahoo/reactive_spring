package org.example.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.example.api.service.StockPriceStreamService;
import org.example.beans.StockPrice;
import org.example.config.StockPriceSerde;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Service;

@Configuration
@EnableKafkaStreams
public class ConsumerStreams {
    // create a bean
    // read from a topic
    // process the data --> for e.g., filter
    // write to another stream
    private final StockPriceStreamService priceStreamService;

    public ConsumerStreams(StockPriceStreamService priceStreamService) {
        this.priceStreamService = priceStreamService;
    }

    @Bean
    public KStream<String, StockPrice> stockPricesStream(@Value("${kafka-topics.stock-prices}") String stockPricesTopic,
                                                         @Value("${kafka-topics.invalid-stock-prices}") String invalidPricesTopic,
                                                         StreamsBuilder builder) {
        KStream<String, StockPrice> stream = builder
                .stream(stockPricesTopic, Consumed.with(Serdes.String(), new StockPriceSerde()));

        stream.filter((k, v) -> {
                    System.out.println(v);
                    priceStreamService.publish(v);
                    return v.price() <= 0.0;
                })
                .to(invalidPricesTopic);

        return stream;
    }
}
