package org.example.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.rsocket.autoconfigure.RSocketProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import reactor.core.publisher.Mono;

import java.net.URI;

@Configuration
public class RSocketConfiguration {

    @Bean
    public Mono<RSocketRequester> rSocketRequester(
            RSocketStrategies rSocketStrategies,
            RSocketProperties rSocketProps) {
        return RSocketRequester.builder()
                .rsocketStrategies(rSocketStrategies)
                .connectWebSocket(getURI(rSocketProps));
    }

    private URI getURI(RSocketProperties rSocketProps) {
        return URI.create(
                    String.format("ws://localhost:%d%s",
                    rSocketProps.getServer().getPort(),
                    rSocketProps.getServer().getMappingPath()
                )
        );
    }

}