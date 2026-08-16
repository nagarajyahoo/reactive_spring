package org.example.beans;

import org.yaml.snakeyaml.util.Tuple;

import java.util.Random;

public class StockPriceUtil {
    private static String[] stocks = {"JPM", "INFY", "GS", "META", "GOOGL", "OPENAI"};
    private static double[] prices = {100.0, 200.0, 300.0, 400.0, 500.0, 600.0};

    public static Tuple<String, Double> stockPrice() {
        int index = new Random().nextInt(stocks.length);
        return new Tuple<>(stocks[index], prices[index]);
    }
}
