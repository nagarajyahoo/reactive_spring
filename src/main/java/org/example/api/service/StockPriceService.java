package org.example.api.service;

import org.example.beans.StockPrice;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Service
public class StockPriceService {
    private final Sinks.Many<StockPrice> sink =
            Sinks.many()
                    .multicast()
                    .directBestEffort();

    public void publish(StockPrice update) {
        sink.tryEmitNext(update);
    }

    public Flux<StockPrice> stream() {
        return sink.asFlux();
    }

    public Flux<StockPrice> stream(String symbol) {
        return sink.asFlux()
                .filter(update ->
                        update.symbol().equalsIgnoreCase(symbol)
                );
    }
}