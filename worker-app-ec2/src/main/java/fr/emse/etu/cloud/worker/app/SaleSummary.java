package fr.emse.etu.cloud.worker.app;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.*;

/**
 * This class computes statistics to summarize sales.
 */
public class SaleSummary {
    private static final Map<String, SaleSummary> summaries = new HashMap<>();
    private final List<Sale> allSales = new ArrayList<>();
    private final Set<String> stores = new HashSet<>();     // A set allows to only keep unique values
    private final Set<String> products = new HashSet<>();   // which is useful for filtering by set
    private final DecimalFormat currencyFormat = new DecimalFormat("0.00");

    public record Sale(String store, String product, int quantity, float price, float profit) {
    }

    private SaleSummary() {
    }

    public static SaleSummary createOrGetSummary(String date) {
        SaleSummary summary = summaries.get(date);
        if (summary == null) {
            summary = new SaleSummary();
            summaries.put(date, summary);
        }
        return summary;
    }

    /**
     * Parses the CSV file to update the summary
     *
     * @param file File to parse
     * @return a fr.emse.etu.cloud.app.SaleSummary
     */
    public static void parseSales(SaleSummary summary, String file) {
        try (Reader fileReader = new FileReader(file)) {
            // Parse records from CSV
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
                    .setDelimiter(';')
                    .setHeader("Date_Time", "Store", "Product", "Quantity", "Unit_Price", "Unit_Cost", "Unit_Profit", "Total_Price")
                    .setSkipHeaderRecord(true)
                    .build()
                    .parse(fileReader);

            // Iterate over each record in CSV
            for (CSVRecord record : records) {
                String store = record.get("Store");
                String product = record.get("Product");
                int quantity = Integer.parseInt(record.get("Quantity"));
                float unitPrice = Float.parseFloat(record.get("Unit_Price"));
                float unitProfit = Float.parseFloat(record.get("Unit_Profit"));

                // Add record as sale
                summary.addSale(new Sale(store, product, quantity, unitPrice, unitProfit));
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void updateSummaryByStore(SaleSummary summary, String outputFile) {
        try {
            // Create the file object from the output file path
            File file = new File(outputFile);

            // Ensure parent directories exist
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }

            // Using try-with-resources for FileWriter
            try (FileWriter fileWriter = new FileWriter(file)) {
                // Write the header only at the first line
                fileWriter.write("Store;Total_Profit;\n");

                // Write the stores
                for (String store : summary.stores)
                    fileWriter.write(store + ';' + summary.totalProfitByStore(store) + "$;\n");

                System.out.println("[Worker] Data per store successfully updated into " + Path.of(file.toURI()).toUri());
            }
        } catch (IOException e) {
            System.err.println("[Worker] An error occurred while writing to the file.");
            e.printStackTrace();
        }
    }

    public static void updateSummaryByProduct(SaleSummary summary, String outputFile) {
        Path filepath = Path.of(outputFile);

        try (FileWriter fileWriter = new FileWriter(outputFile)) {

            // Write the header only at the first line
            fileWriter.write("Product;Total_Profit;Total_Quantity;Total_Sold\n");
            // Write the summary per product
            for (String product : summary.products)
                fileWriter.write(product + ";" + summary.totalProfitByProduct(product) + "$;"
                        + summary.totalQuantityByProduct(product) + " units;"
                        + "\"" + summary.totalSoldByProduct(product) + "$\";\n");

            System.out.println("[Worker] Data per product successfully updated into " + filepath.toUri());
        } catch (IOException e) {
            System.err.println("[Worker] An error occurred while writing to the file.");
            e.printStackTrace();
        }
    }

    private void addSale(Sale sale) {
        allSales.add(sale);
        products.add(sale.product());
        stores.add(sale.store());
    }

    private String totalProfitByStore(String store) {
        return currencyFormat.format(allSales.stream()
                .filter(t -> t.store().equals(store))
                .mapToDouble(Sale::price)
                .sum());
    }

    private int totalQuantityByProduct(String product) {
        return allSales.stream()
                .filter(t -> t.product().equals(product))
                .mapToInt(Sale::quantity)
                .sum();
    }

    private String totalProfitByProduct(String product) {
        return currencyFormat.format(allSales.stream()
                .filter(t -> t.product().equals(product))
                .mapToDouble(t -> t.quantity() * t.profit())
                .sum());
    }

    private int totalSoldByProduct(String product) {
        return (int) allSales.stream()
                .filter(t -> t.product().equals(product))
                .count();
    }
}
