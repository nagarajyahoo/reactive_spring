package org.example.beans;

import org.yaml.snakeyaml.util.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class StockPriceUtil {
    private static String[] stocks = {"JPM", "INFY", "GS", "META", "GOOGL", "OPENAI"};
    private static double[] prices = {100.0, 200.0, 300.0, 400.0, 500.0, 600.0};
    private static String[] signals = {"BUY", "SELL", "HOLD"};

    public static Tuple<String, Double> stockPrice() {
        int index = new Random().nextInt(stocks.length);
        return new Tuple<>(stocks[index], prices[index]);
    }

    public static List<StockPrice> stockPrices(int count) {
        List<StockPrice> stockPrices = new ArrayList<>();
        Random random = new Random();

        while(count-- > 0) {
            int idx = random.nextInt(stocks.length);
            double priceChange = random.nextDouble(-0.03, 0.03) + 1.0;
            stockPrices.add(new StockPrice(stocks[idx], priceChange * prices[idx], System.currentTimeMillis()));
        }
        return stockPrices;
    }

    public static List<StockSignal> stockSignals(int count) {
        List<StockSignal> stockSignals = new ArrayList<>();
        Random random = new Random();

        while(count-- > 0) {
            int idx = random.nextInt(stocks.length);
            int sigIdx = random.nextInt(signals.length);
            stockSignals.add(new StockSignal(stocks[idx], signals[sigIdx], System.currentTimeMillis()));
        }
        return stockSignals;
    }
}
