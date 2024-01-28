package fr.emse.etu.cloud.worker.app;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.*;
import java.text.DecimalFormat;
import java.util.*;

/**
 * This class computes statistics to summarize sales.
 */
public class SaleSummary {
    private final List<Sale> allSales = new ArrayList<>();
    private final Set<String> stores = new HashSet<>();     // A set allows to only keep unique values
    private final Set<String> products = new HashSet<>();   // which is useful for filtering by set
    private final DecimalFormat currencyFormat = new DecimalFormat("0.00");

    public record Sale(String store, String product, int quantity, float price, float profit) {
    }

    /**
     * Parses the CSV file to update the summary
     *
     * @return a fr.emse.etu.cloud.app.SaleSummary
     */
    public static SaleSummary parseSales(String bucketName, String s3ObjectKey) {
        SaleSummary summary = new SaleSummary();

        S3Client s3Client = S3Client.builder().build();
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3ObjectKey)
                .build();

        try (ResponseInputStream<GetObjectResponse> s3Input = s3Client.getObject(getObjectRequest);
             BufferedReader reader = new BufferedReader(new InputStreamReader(s3Input))) {

            Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
                    .setDelimiter(';')
                    .setHeader("Date_Time", "Store", "Product", "Quantity", "Unit_Price", "Unit_Cost", "Unit_Profit", "Total_Price")
                    .setSkipHeaderRecord(true)
                    .build()
                    .parse(reader);

            for (CSVRecord record : records) {
                String store = record.get("Store");
                String product = record.get("Product");
                int quantity = Integer.parseInt(record.get("Quantity"));
                float unitPrice = Float.parseFloat(record.get("Unit_Price"));
                float unitProfit = Float.parseFloat(record.get("Unit_Profit"));

                // Add record as sale to summary
                summary.addSale(new Sale(store, product, quantity, unitPrice, unitProfit));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return summary;
    }

    /**
     * Update the store summary, it creates the file if nonexistent, and append the store profit otherwise
     * @param summary
     * @param outputFile
     */
    public static void updateSummaryByStore(SaleSummary summary, String outputFile) {
        try {
            // Create the file object from the output file path
            File file = new File(outputFile);

            // Ensure parent directories exist
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdirs();
            }

            // Using try-with-resources for FileWriter
            try (FileWriter fileWriter = new FileWriter(file, file.exists())) {
                // Write the header only at the first line
                if(!file.exists())
                    fileWriter.write("Store;Total_Profit;\n");

                // Write the stores
                for (String store : summary.stores)
                    fileWriter.write(store + ';' + summary.totalProfitByStore(store) + "$;\n");

                System.out.println("[Worker] Data per store successfully updated into " + file.toPath().toUri());
            }
        } catch (IOException e) {
            System.err.println("[Worker] An error occurred while writing to the file.");
            e.printStackTrace();
        }
    }

    /**
     * Unlike store summary, this will rewrite the whole file with the recalculated products totals
     * @param summary
     * @param outputFile
     */
    public static void updateSummaryByProduct(SaleSummary summary, String outputFile, Set<Product> products) {

        // Create the file object from the output file path
        File file = new File(outputFile);

        // Ensure parent directories exist
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }

        try (FileWriter fileWriter = new FileWriter(outputFile)) {

            // Write the header only at the first line
            fileWriter.write("Product;Total_Profit;Total_Quantity;Total_Sold\n");
            // Write the summary per product
            for (String product : summary.products) {
                Product productStat = products.stream()
                        .filter(p -> p.productName().equals(product))
                        .findFirst()
                        .orElse(new Product(product, 0, 0, 0));

                try {
                    fileWriter.write("%s;%s$;%s units;\"%s$\";\n".formatted(product,
                            summary.totalProfitByProduct(product) + productStat.totalProfit(),
                            summary.totalQuantityByProduct(product) + productStat.totalQuantity(),
                            summary.totalSoldByProduct(product) + productStat.totalSold())
                    );
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            System.out.println("[Worker] Data per product successfully updated into " + file.toPath().toUri());
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
