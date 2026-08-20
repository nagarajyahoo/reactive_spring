package org.example.api.service;

import org.example.beans.StockPrice;
import org.example.beans.StockSignal;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@Service
public class StockSignalsService {
    private final Sinks.Many<StockSignal> sink =
            Sinks.many()
                    .multicast()
                    .directBestEffort();

    public void publish(StockSignal update) {
        sink.tryEmitNext(update);
    }

    public Flux<StockSignal> stream() {
        return sink.asFlux();
    }

    public Flux<StockSignal> stream(String symbol) {
        return sink.asFlux()
                .filter(update ->
                        update.symbol().equalsIgnoreCase(symbol)
                );
    }
}
