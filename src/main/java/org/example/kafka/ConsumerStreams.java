package org.example.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.example.api.service.StockPriceService;
import org.example.api.service.StockSignalsService;
import org.example.beans.StockPrice;
import org.example.beans.StockSignal;
import org.example.config.StockPriceSerde;
import org.example.config.StockSignalSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Set;

@Configuration
@EnableKafkaStreams
public class ConsumerStreams {
    private static final Logger log = LoggerFactory.getLogger(ConsumerStreams.class);
    // create a bean
    // read from a topic
    // process the data --> for e.g., filter
    // write to another stream
    private final StockPriceService priceStreamService;
    private final StockSignalsService signalService;
    private final Set<String> valid_signals = Set.of("BUY", "SELL", "HOLD");

    public ConsumerStreams(StockPriceService priceStreamService,
                           StockSignalsService signalService) {
        this.priceStreamService = priceStreamService;
        this.signalService = signalService;
    }

    @Bean
    public KStream<String, StockPrice> stockPricesStream(@Value("${kafka-topics.stock-prices}") String stockPricesTopic,
                                                         @Value("${kafka-topics.invalid-stock-prices}") String invalidPricesTopic,
                                                         StreamsBuilder builder) {
        KStream<String, StockPrice> stream = builder
                .stream(stockPricesTopic, Consumed.with(Serdes.String(), new StockPriceSerde()));

        stream.filter((k, v) -> {
                    log.info("[stock_price]: Received {}", v);
                    priceStreamService.publish(v);
                    return v.price() <= 0.0;
                })
                .to(invalidPricesTopic);

        return stream;
    }

    @Bean
    public KStream<String, StockSignal> stockSignalsStream(@Value("${kafka-topics.stock-signals}") String stockSignalsTopic,
                                                         @Value("${kafka-topics.invalid-stock-signals}") String invalidSignalsTopic,
                                                         StreamsBuilder builder) {
        KStream<String, StockSignal> stream = builder
                .stream(stockSignalsTopic, Consumed.with(Serdes.String(), new StockSignalSerde()));

        stream.filter((k, v) -> {
                    log.info("[stock_signal]: Received {}", v);
                    signalService.publish(v);
                    return v.signal() == null || !valid_signals.contains(v.signal().toUpperCase());
                })
                .to(invalidSignalsTopic);

        return stream;
    }
}
