package org.example.kafka.api;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.beans.StockPrice;
import org.example.beans.StockPriceUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.apache.kafka.common.serialization.Serializer;
import org.yaml.snakeyaml.util.Tuple;

import java.util.Random;

@RestController
@RequestMapping("/publisher")
public class KafkaPublisher {
    private final String stockPricesTopic;
    private final KafkaTemplate<String, StockPrice> producer;

    public KafkaPublisher(@Value("${kafka-topics.stock-prices}") String stockPricesTopic,
                          KafkaTemplate<String, StockPrice> producer) {
        this.stockPricesTopic = stockPricesTopic;
        this.producer = producer;

    }

    @PostMapping("/publish/{messages_count}")
    public String publishMessages(@PathVariable("messages_count") int numMessages) {
        int i = 0;
        while (i++ < numMessages) {
            producer.send(new ProducerRecord<>(stockPricesTopic, stockPrice()));
        }
        return "done-" + numMessages;
    }

    private StockPrice stockPrice() {
        Tuple<String, Double> stockPrice = StockPriceUtil.stockPrice();
        return new StockPrice(stockPrice._1(), stockPrice._2(), System.currentTimeMillis());
    }
}
