package fr.emse.etu.cloud.worker.app;

import fr.emse.etu.cloud.worker.s3.S3DownloadObject;
import fr.emse.etu.cloud.worker.s3.S3UploadObject;
import fr.emse.etu.cloud.worker.sqs.*;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.File;
import java.util.List;

/**
 * The application When receiving an SQS Message in `INBOX` queue, downloads sales files from S3, summarizes sales, and
 * uploads summary.
 * <p>
 * It summarizes the daily sales by store and by product:
 * - By Store : total profit,
 * - By Product: total quantity, total sold, total profit.
 */
public class WorkerApp {

    public static final String BUCKETNAME = "thatbucket95110";
    public static final String INBOX = "INBOX";
    public static final String OUTBOX = "OUTBOX";
    public static final String S3PATHFOLDER_SUMMARY = "summary/";
    public static final String WORKERFOLDER_SALES = "data/worker/sales/";
    public static final String WORKERFOLDER_SUMMARY = "data/worker/summary/";
    public static final String WORKER_STORE_SUMMARY = "summaryByStore.csv";
    public static final String WORKER_PRODUCT_SUMMARY = "summaryByProduct.csv";


    public static void main(String[] args) throws InterruptedException {
        // Check queue if they exist
        checkQueueExist(INBOX);
        checkQueueExist(OUTBOX);

        // Loop the received message
        List<Message> messages;
        System.out.println("[Worker] Listening INBOX queue for messages every 10s");
        while (true) {
            messages = SQSReceiveMessage.receiveMessages(INBOX);
            if (!messages.isEmpty()) {
                for (Message message : messages) {
                    // For each message, extract filename and download the file
                    String bucketName = message.body().split(":")[0];
                    String filePath = message.body().split(":")[1];
                    String fileName = message.body().split("/")[1];

                    // Download object on S3 given SQS msg
                    S3DownloadObject.downloadObject(bucketName, filePath, WORKERFOLDER_SALES + fileName);
                    // Update summary on the Worker once all available files downloaded
                    updateSummary(fileName);
                }
                // Delete message on the SQS queue once received
                SQSDeleteMessage.deleteMessages(INBOX, messages);

                // Notifying OUTBOX queue
                System.out.println("[Worker] Notifying OUTBOX queue");
                String[] files = new File(WORKERFOLDER_SUMMARY).list();
                if (files != null) {
                    for (String file : files) {
                        S3UploadObject.uploadObject(BUCKETNAME, WORKERFOLDER_SUMMARY + file, S3PATHFOLDER_SUMMARY + file, true);
                        SQSSendMessage.sendMessages(OUTBOX, BUCKETNAME + ":" + S3PATHFOLDER_SUMMARY + file);
                    }
                }
            }
            System.out.println("\n[Worker] Now listening for messages from INBOX queue every 10s");
            Thread.sleep(10000);
        }
    }

    private static void checkQueueExist(String queue) {
        if (!SQSCheckQueue.exists(queue))
            SQSCreateQueue.createQueue(queue);
        System.out.println("[Worker] "+ queue + " queue has been set up.");
    }

    private static void updateSummary(String file) {
        // Parse dates from file names '01-10-2022-store1.csv' => '01-10-2022'
        String date = file.substring(0, 10);
        // Get summary by date
        SaleSummary summary = SaleSummary.createOrGetSummary(date);
        // Parse sales
        SaleSummary.parseSales(summary, WORKERFOLDER_SALES + file);
        // Update summary by store
        SaleSummary.updateSummaryByStore(summary, WORKERFOLDER_SUMMARY + date + '-' + WORKER_STORE_SUMMARY);
        // Update summary by products
        SaleSummary.updateSummaryByProduct(summary, WORKERFOLDER_SUMMARY + date + '-' + WORKER_PRODUCT_SUMMARY);
    }
}
