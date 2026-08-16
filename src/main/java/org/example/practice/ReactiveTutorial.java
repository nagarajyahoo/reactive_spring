package org.example.practice;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class ReactiveTutorial {
    private Mono<String> monoString(boolean notNull) {
        String str = notNull ? "hello world !!!" : null;
        return Mono.justOrEmpty(str)
                .log();
    }

    private Flux<String> fluxString() {
        return Flux
                .fromIterable(List.of("java", "cpp", "python", "javascript"))
                .map(x -> "X:" + x.toUpperCase())
                .log();
    }

    private Mono<List<String>> fluxStringCollection() {
        return Flux
                .fromIterable(List.of("java", "cpp", "python", "javascript"))
                .delayElements(Duration.ofMillis(1000))
                .collectList()
                .log();
    }

    private Flux<String> flatMapTest() {
        return Flux
                .fromIterable(List.of("java", "cpp", "python", "javascript"))
                .filter(x -> x.contains("j"));
//                .log();
    }

    private Flux<Tuple3<Integer, Integer, Integer>> intFlux() {
        Flux<Integer> x = Flux.range(1, 10).delayElements(Duration.ofMillis(500));
        Flux<Integer> y = Flux.range(11, 10).delayElements(Duration.ofMillis(10));
        Flux<Integer> z = Flux.range(21, 10).delayElements(Duration.ofMillis(800));
        return Flux.zip(x, y, z);
    }

    private Flux<List<Integer>> testBuffer() {
        return Flux.range(0, 10)
                .delayElements(Duration.ofMillis(500))
                .buffer(Duration.ofMillis(1_100));
    }

    private Mono<Map<Integer, Integer>> testMap() {
        return Flux.range(0, 10)
                .delayElements(Duration.ofMillis(500))
                .collectMap(x -> x, x -> x * x);
    }

    private Flux<Integer> intRangeTest() {
        return Flux.range(0, 10)
                .map(x -> {
                    if(x == 5) throw new RuntimeException("runtime error");
                    else return x*x;
                })
                .onErrorResume(e -> Flux.range(100, 5))
                .log();
    }

    public static void main(String[] args) throws Exception {
        ReactiveTutorial obj = new ReactiveTutorial();
//        obj.monoString(false).subscribe(data -> System.out.println(data));
//        obj.intFlux().subscribe(data -> System.out.printf("received: %s\n", data));
//        obj.fluxStringCollection().subscribe(data -> System.out.printf("Rx: %s\n", data));
        obj.intRangeTest().subscribe(x -> System.out.printf("rec: %s\n", x));
        Thread.sleep(10_000);
    }
}
