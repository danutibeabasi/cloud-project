package fr.emse.etu.cloud.consolidator.app;

import fr.emse.etu.cloud.consolidator.s3.S3DownloadObject;
import fr.emse.etu.cloud.consolidator.sqs.SQSDeleteMessage;
import fr.emse.etu.cloud.consolidator.sqs.SQSReceiveMessage;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.List;

/**
 * The application, when receiving an SQS Message in `OUTBOX` queue, downloads summary files from S3,
 * analyzes them saving them locally.
 *
 * It reads the summary results from the files of that date and computes: the total retailer’s profit,the most and least
 * profitable stores, and the total quantity, total sold, and total profit per product.
 */
public class ConsolidatorApp {

    public static final String OUTBOX = "OUTBOX";
    public static final String LOCALFOLDER_STATS = "data/consolidator/";
    public static final String LOCALNAME_ANALYSIS = "analysisResults.txt";


    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("[Consolidator] Please enter the correct argument: <date>");
            System.exit(1);
        }
        run(args[0]);
    }


    /**
     * Runs the Consolidator App which analyzes the summary data of the specified date.
     * @param date Date to input for the analysis
     */
    public static void run(String date) {
        // Loop to check incoming messages every 10 seconds
        System.out.println("[Consolidator] Checking queue for messages every 10 seconds:");
        List<Message> messages;
        while (!(messages = SQSReceiveMessage.receiveMessages(OUTBOX)).isEmpty()) {

            for (Message message : messages) {
                String bucketName = message.body().split(":")[0];
                String filePath = message.body().split(":")[1];
                String fileName = message.body().split("/")[1];
                S3DownloadObject.downloadObject(bucketName, filePath, LOCALFOLDER_STATS + fileName);
            }
            SQSDeleteMessage.deleteMessages(OUTBOX, messages);
        }

        String[] files = new File(LOCALFOLDER_STATS).list((dir,file) -> file.startsWith(date) && !file.endsWith(LOCALNAME_ANALYSIS));
        if (files != null) {
            String summaryByProductFile = files[0];
            String summaryByStoreFile = files[1];
            analyze(LOCALFOLDER_STATS+summaryByStoreFile, LOCALFOLDER_STATS+summaryByProductFile,
                    LOCALFOLDER_STATS+ date + '-' + LOCALNAME_ANALYSIS);
        }

    }

    /**
     * Analyzes statistics to extract
     * @param summaryByStore
     */
    public static void analyze(String summaryByStore, String summaryByProduct, String outputFile) {
        System.out.println("[Consolidator] Analysing CSV data for minimum, maximum, and sum calculations");

        float maxProfit = 0;
        float minProfit = Float.MAX_VALUE;
        float totalProfit = 0;
        String mostProfitableStore = null, leastProfitableStore = null;

        // Reads and process store CSV
        try (Reader in = new FileReader(summaryByStore)) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setDelimiter(';')
                    .setHeader("Store", "Total Profit")
                    .setSkipHeaderRecord(true)
                    .build();
            Iterable<CSVRecord> records = csvFormat.parse(in);


            // It reads the summary results from the files of that date and computes:
            // the total retailer’s profit,the most and leastprofitable stores,
            for (CSVRecord record : records) {
                String store = record.get("Store");
                String profitRecord = record.get("Total Profit");
                // Remove '$' suffix from "17535.38$"
                float profit = Float.parseFloat(profitRecord.substring(0, profitRecord.length() - 1));

                totalProfit += profit;
                if (profit > maxProfit) {
                    maxProfit = profit;
                    mostProfitableStore = store;
                }
                if (profit < minProfit) {
                    minProfit = profit;
                    leastProfitableStore = store;
                }
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        // Process product CSV
        List<CSVRecord> productRecords;
        try (Reader productReader = new FileReader(summaryByProduct)) {
             CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                     .setDelimiter(';')
                     .setHeader("Product", "Total_Profit", "Total_Quantity", "Total_Sold")
                     .setSkipHeaderRecord(true)
                     .build();
             productRecords = csvFormat.parse(productReader).getRecords();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }



        // Writing analysis results
        Path path = Path.of(outputFile);
        DecimalFormat formatter = new DecimalFormat("#.00");
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write("\tData Analysis Results: " + outputFile + ":\n");
            writer.write("\tTotal Retailer's Profit: " + formatter.format(totalProfit)+"$\n");
            writer.write("\tMost Profitable Store: " + mostProfitableStore + "\n");
            writer.write("\tLeast Profitable Store: " + leastProfitableStore + "\n");

            for (CSVRecord record : productRecords) {
                writer.write("\tProduct: " + record.get("Product") + " \tTotal Quantity: " + record.get("Total_Quantity")
                        + "\tTotal Sold: " + record.get("Total_Sold") + "\tTotal Profit: " + record.get("Total_Profit") + "\n"
                );
            }
        } catch (IOException e) {
            System.out.println("An error occurred while writing to the file " + path.toUri());
            e.printStackTrace();
        }

        try {
            List<String> lines = Files.readAllLines(path);
            for (String fileLine : lines)
                System.out.println(fileLine);

            System.out.println("Data analysis results successfully written into " + path.toUri());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
