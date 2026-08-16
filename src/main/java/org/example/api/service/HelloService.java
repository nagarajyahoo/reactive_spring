package org.example.api.service;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.example.beans.StockPrice;
import org.example.config.StockPriceSerde;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Service
public class HelloService {
    private final StockPriceStreamService priceStreamService;

    public HelloService(StockPriceStreamService priceStreamService) {
        this.priceStreamService = priceStreamService;
    }

    public List<String> getPrices(){
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add("price-" + i);
        }
        return list;
    }

    public Flux<StockPrice> priceStream() {
        return priceStreamService.stream();
    }

    public Flux<StockPrice> priceStream(String symbol) {
        return priceStreamService.stream(symbol);
    }
}
