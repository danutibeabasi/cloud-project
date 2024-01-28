package fr.emse.etu.cloud.worker.app;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import fr.emse.etu.cloud.worker.s3.S3DownloadObject;
import fr.emse.etu.cloud.worker.s3.S3UploadObject;

import java.io.File;
import java.util.Set;

/**
 * The application When receiving an SQS Message in `INBOX` queue, downloads sales files from S3, summarizes sales, and
 * uploads summary.
 * <p>
 * It summarizes the daily sales by store and by product:
 * - By Store : total profit,
 * - By Product: total quantity, total sold, total profit.
 */
public class WorkerApp implements RequestHandler<S3Event, Void> {

    public static final String BUCKETNAME = "thatbucket95110";
    public static final String INBOX = "INBOX";
    public static final String OUTBOX = "OUTBOX";
    public static final String S3PATHFOLDER_SUMMARY = "summary/";
    public static final String WORKERFOLDER_SALES = "tmp/worker/sales/";
    public static final String WORKERFOLDER_SUMMARY = "tmp/worker/summary/";
    public static final String WORKER_STORE_SUMMARY = "summaryByStore.csv";
    public static final String WORKER_PRODUCT_SUMMARY = "summaryByProduct.csv";

    public Void handleRequest(S3Event event, Context context) {
        try {
            S3EventNotification.S3EventNotificationRecord notificationRecord = event.getRecords().get(0);
            String bucketName = notificationRecord.getS3().getBucket().getName();
            String s3ObjectKey = notificationRecord.getS3().getObject().getUrlDecodedKey();
            String fileName = s3ObjectKey.split("/")[1];
            String date = fileName.substring(0,10);

            S3DownloadObject.downloadObject(bucketName, s3ObjectKey, WORKERFOLDER_SALES + fileName);

            // Parse sales file
            SaleSummary summary = SaleSummary.parseSales(bucketName, s3ObjectKey);

            // Update summary on the Worker once all available files downloaded
            updateSummary(summary, date);

            String[] files = new File(WORKERFOLDER_SUMMARY).list();
            if (files != null)
                for (String file : files)
                    S3UploadObject.uploadObject(BUCKETNAME, WORKERFOLDER_SUMMARY + file, S3PATHFOLDER_SUMMARY + file, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

//    private static void uploadSummary() {
//        String[] filenames = new File(WORKERFOLDER_SUMMARY).list();
//        List<File> files = null;
//        if (filenames != null)
//            for (String filename : filenames)
//                files.add(new File(WORKERFOLDER_SUMMARY +filename), S3PATHFOLDER_SUMMARY+filename)
//    }

    private static void updateSummary(SaleSummary summary, String date) {
        String productSummaryFile = WORKERFOLDER_SUMMARY + date + '-' + WORKER_PRODUCT_SUMMARY;
        // Update summary by store
        SaleSummary.updateSummaryByStore(summary, "%s%s-%s".formatted(WORKERFOLDER_SUMMARY, date, WORKER_STORE_SUMMARY));
        // Parse current stats on products
        Set<Product> product = Product.parseProducts(productSummaryFile);
        // Update summary by products
        SaleSummary.updateSummaryByProduct(summary, "%s%s-%s".formatted(WORKERFOLDER_SUMMARY, date, WORKER_PRODUCT_SUMMARY),
                product);


    }
}
