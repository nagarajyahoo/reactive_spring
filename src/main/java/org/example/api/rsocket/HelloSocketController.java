package org.example.api.rsocket;

import org.example.api.service.HelloService;
import org.example.beans.StockPrice;
import org.example.beans.StockSignal;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Controller
public class HelloSocketController {
    private final HelloService helloService;

    public HelloSocketController(HelloService helloService) {
        this.helloService = helloService;
    }

    @MessageMapping("stock.hello")
    public Mono<String> hello(String message) {
        return Mono.just(
                "Server received: " + message
        );
    }

    @MessageMapping("stock.prices")
    public Flux<String> prices() {
        return Flux.fromIterable(helloService.getPrices())
                .delayElements(Duration.ofSeconds(1))
                .log();
    }

    @MessageMapping("stock.prices.stream")
    public Flux<StockPrice> pricesStream(String symbol) {
        return helloService.priceStream(symbol)
                .delayElements(Duration.ofSeconds(1))
                .log();
    }

    @MessageMapping("stock.signals.stream")
    public Flux<StockSignal> pricesSignals(String symbol) {
        return helloService.signalStream(symbol)
                .delayElements(Duration.ofSeconds(1))
                .log();
    }
}
