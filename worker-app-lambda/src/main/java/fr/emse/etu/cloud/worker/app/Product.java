package fr.emse.etu.cloud.worker.app;

import java.util.HashSet;
import java.util.Set;

public record Product(String productName, double totalProfit, int totalQuantity, double totalSold) {

    public static Set<Product> parseProducts(String csvContent) {
        Set<Product> productSummaries = new HashSet<>();
        String[] lines = csvContent.split("\n");
        for (String line : lines) {
            if (line.startsWith("Product")) continue; // Skip header
            String[] parts = line.split(";");
            String productName = parts[0];
            double totalProfit = Double.parseDouble(parts[1].replace("$", ""));
            int totalQuantity = Integer.parseInt(parts[2].split(" ")[0]);
            double totalSold = Double.parseDouble(parts[3].replace("\"", "").replace("$", ""));
            productSummaries.add(new Product(productName, totalProfit, totalQuantity, totalSold));
        }
        return productSummaries;
    }

}
