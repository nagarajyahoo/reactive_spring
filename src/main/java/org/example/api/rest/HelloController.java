package org.example.api.rest;

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

    @GetMapping("/prices")
    public Flux<String> getCount() {
        return requester
                .flatMapMany(req -> req.route("stock.prices").retrieveFlux(String.class));
    }

    @GetMapping("/priceStream/{symbol}")
    public Flux<String> priceStream(@PathVariable("symbol") String symbol) {
        return requester
                .flatMapMany(
                        req -> req.route("stock.prices.stream")
                                .data(symbol)
                                .retrieveFlux(StockPrice.class)
                                .map(objectMapper::writeValueAsString)
                );
    }

    @GetMapping("/signalsStream/{symbol}")
    public Flux<String> signalStream(@PathVariable("symbol") String symbol) {
        return requester
                .flatMapMany(
                        req -> req.route("stock.signals.stream")
                                .data(symbol)
                                .retrieveFlux(StockSignal.class)
                                .map(objectMapper::writeValueAsString)
                );
    }
}
