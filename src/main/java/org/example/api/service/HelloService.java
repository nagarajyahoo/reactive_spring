package org.example.api.service;

import org.example.beans.StockPrice;
import org.example.beans.StockPriceUtil;
import org.example.beans.StockSignal;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

@Service
public class HelloService {
    private final StockPriceService priceStreamService;
    private final StockSignalsService stockSignalsService;

    public HelloService(StockPriceService priceStreamService,
                        StockSignalsService stockSignalsService) {
        this.priceStreamService = priceStreamService;
        this.stockSignalsService = stockSignalsService;
    }

    public Flux<StockPrice> priceStream(int count) {
        return Flux.fromIterable(StockPriceUtil.stockPrices(count));
    }

    public Flux<StockSignal> signalStream(int count) {
        return Flux.fromIterable(StockPriceUtil.stockSignals(count));
    }

    public Flux<StockPrice> priceStream(String symbol) {
        return priceStreamService.stream(symbol);
    }

    public Flux<StockSignal> signalStream(String symbol) {
        return stockSignalsService.stream(symbol);
    }
}
