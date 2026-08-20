package org.example.kafka.api;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.beans.StockSignal;
import org.example.beans.StockPrice;
import org.example.beans.StockPriceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.yaml.snakeyaml.util.Tuple;

@RestController
@RequestMapping("/publisher")
public class KafkaPublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaPublisher.class);
    private final String stockPricesTopic;
    private final String stockSignalsTopic;
    private final KafkaTemplate<String, StockPrice> priceProducer;
    private final KafkaTemplate<String, StockSignal> signalProducer;


    public KafkaPublisher(@Value("${kafka-topics.stock-prices}") String stockPricesTopic,
                          @Value("${kafka-topics.stock-signals}") String stockSignalsTopic,
                          @Qualifier("stockPricesKafkaTemplate") KafkaTemplate<String, StockPrice> priceProducer,
                          @Qualifier("stockSignalsKafkaTemplate") KafkaTemplate<String, StockSignal> signalProducer) {
        this.stockPricesTopic = stockPricesTopic;
        this.stockSignalsTopic = stockSignalsTopic;
        this.priceProducer = priceProducer;
        this.signalProducer = signalProducer;

    }

    @PostMapping("/publish/{messages_count}")
    public String publishMessages(@PathVariable("messages_count") int numMessages) {
        int i = 0;
        while (i++ < numMessages) {
            priceProducer.send(new ProducerRecord<>(stockPricesTopic, stockPrice()));
        }
        return "done-" + numMessages;
    }

    @PostMapping("/publish/stock-price")
    public String publishPrice(@RequestBody StockPrice x) {
        try {
            StockPrice stockPrice = new StockPrice(x.symbol(), x.price(), System.currentTimeMillis());
            priceProducer.send(new ProducerRecord<>(stockPricesTopic, stockPrice));
            return "done";
        } catch(Exception e) {
            log.error("Error: ", e);
            return "failed";
        }
    }

    @PostMapping("/publish/stock-signal")
    public String publishSignal(@RequestBody StockSignal x) {
        try {
            StockSignal signal = new StockSignal(x.symbol(), x.signal(), System.currentTimeMillis());
            signalProducer.send(new ProducerRecord<>(stockSignalsTopic, signal));
            return "done";
        } catch(Exception e) {
            log.error("Error: ", e);
            return "failed";
        }
    }

    private StockPrice stockPrice() {
        Tuple<String, Double> stockPrice = StockPriceUtil.stockPrice();
        return new StockPrice(stockPrice._1(), stockPrice._2(), System.currentTimeMillis());
    }
}
