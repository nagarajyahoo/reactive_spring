package org.example.api.service;

import org.example.beans.StockPrice;
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

    public Flux<StockSignal> signalStream(String symbol) {
        return stockSignalsService.stream(symbol);
    }
}
