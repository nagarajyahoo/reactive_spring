package org.example.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.example.beans.StockPrice;
import org.example.beans.StockSignal;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {
    @Bean
    public NewTopic stockPricesTopic(@Value("${kafka-topics.stock-prices}") String topicName) {
        return new NewTopic(topicName, 3, (short)1);
    }

    @Bean
    public NewTopic stockSignalsTopic(@Value("${kafka-topics.stock-signals}") String topicName) {
        return new NewTopic(topicName, 3, (short)1);
    }

    @Bean
    @Qualifier("stockPricesProducerProperties")
    @ConfigurationProperties("spring.kafka.producers.stock-prices")
    public Map<String, Object> stockPricesProducerProperties() {
        return new HashMap<>();
    }

    @Bean
    @Qualifier("stockPricesProducerFactory")
    public ProducerFactory<String, StockPrice> stockPricesProducerFactory(
            @Qualifier("stockPricesProducerProperties") Map<String, Object> properties) {
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    @Qualifier("stockPricesKafkaTemplate")
    public KafkaTemplate<String, StockPrice> stockPricesKafkaTemplate(
            @Qualifier("stockPricesProducerFactory") ProducerFactory<String, StockPrice> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    // signals template
    @Bean
    @Qualifier("stockSignalsProducerProperties")
    @ConfigurationProperties("spring.kafka.producers.stock-signals")
    public Map<String, Object> stockSignalsProducerProperties() {
        return new HashMap<>();
    }

    @Bean
    @Qualifier("stockSignalsProducerFactory")
    public ProducerFactory<String, StockSignal> stockSignalsProducerFactory(
            @Qualifier("stockSignalsProducerProperties") Map<String, Object> properties) {
        return new DefaultKafkaProducerFactory<>(properties);
    }

    @Bean
    @Qualifier("stockSignalsKafkaTemplate")
    public KafkaTemplate<String, StockSignal> stockSignalsKafkaTemplate(
            @Qualifier("stockSignalsProducerFactory") ProducerFactory<String, StockSignal> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

}
