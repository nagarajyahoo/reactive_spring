package org.example.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
    @Bean
    public NewTopic stockPricesTopic(@Value("${kafka-topics.stock-prices}") String topicName) {
        return new NewTopic(topicName, 3, (short)1);
    }
}
