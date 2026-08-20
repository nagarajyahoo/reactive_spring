package org.example.api.rest;

import org.example.api.rsocket.StreamCountReq;
import org.example.api.service.HelloService;
import org.example.beans.StockPrice;
import org.example.beans.StockSignal;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;

@RestController
@RequestMapping("/hello")
public class HelloController {
    private final HelloService helloService;
    private final Mono<RSocketRequester> requester;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Flux<String> share;

    public HelloController(HelloService helloService,
                           Mono<RSocketRequester> requester) {
        this.helloService = helloService;
        this.requester = requester;
        this.share = Flux.range(0, 1_000_0000)
                .delayElements(Duration.ofSeconds(1))
                .map(i -> " price-" + i)
                .share();
    }

    @GetMapping("/shared")
    public Flux<String> shared() {
        return share;
    }

    @GetMapping("/prices/{count}")
    public Flux<StockPrice> priceStream(@PathVariable("count") int count) {
        return requester
                .flatMapMany(
                        req -> req.route("stock.prices")
                                .data(new StreamCountReq(count))
                                .retrieveFlux(StockPrice.class)
                );
    }

    @GetMapping("/signals/{count}")
    public Flux<StockSignal> signalStream(@PathVariable("count") int count) {
        return requester
                .flatMapMany(
                        req -> req.route("stock.signals")
                                .data(new StreamCountReq(count))
                                .retrieveFlux(StockSignal.class)
                );
    }

    @GetMapping("/priceStream/{symbol}")
    public Flux<StockPrice> priceStream(@PathVariable("symbol") String symbol) {
        return requester
                .flatMapMany(
                        req -> req.route("stock.prices.stream")
                                .data(symbol)
                                .retrieveFlux(StockPrice.class)
                );
    }

    @GetMapping("/signalsStream/{symbol}")
    public Flux<StockSignal> signalStream(@PathVariable("symbol") String symbol) {
        return requester
                .flatMapMany(
                        req -> req.route("stock.signals.stream")
                                .data(symbol)
                                .retrieveFlux(StockSignal.class)
                );
    }
}
