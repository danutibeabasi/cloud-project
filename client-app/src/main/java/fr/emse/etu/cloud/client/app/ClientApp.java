package fr.emse.etu.cloud.client.app;

import fr.emse.etu.cloud.client.s3.S3CheckBucket;
import fr.emse.etu.cloud.client.s3.S3CreateBucket;
import fr.emse.etu.cloud.client.s3.S3UploadObject;
import fr.emse.etu.cloud.client.sqs.SQSCheckQueue;
import fr.emse.etu.cloud.client.sqs.SQSCreateQueue;
import fr.emse.etu.cloud.client.sqs.SQSSendMessage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The Client Application uploads files to Amazon S3 buckets and sends messages to Amazon SQS queues.
 */
public class ClientApp {

    public static final String BUCKET_NAME = "thatbucket95110";
    public static final String QUEUE_NAME = "INBOX";
    public static final String S3PATHFOLDER = "data/";

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("[Client] Please enter the correct argument: <local_filepath>");
            System.exit(1);
        }

        Path path = Paths.get(args[0]);
        if (Files.exists(path)) {
            System.out.println("[Client] Found local file: " + path.toUri());
            run(path);
        } else {
            System.err.println("[Client] Local file not found: " + path.toUri());
        }
    }

    /**
     * Runs the Client App
     * @param path Local file path to uploads to S3 Bucket
     */
    public static void run(Path path) {
        String s3path = S3PATHFOLDER + path.getFileName();

        // Create bucket or queue if they don't exist
        if (!S3CheckBucket.exists(BUCKET_NAME))
            S3CreateBucket.createBucket(BUCKET_NAME);
        if (!SQSCheckQueue.exists(QUEUE_NAME))
            SQSCreateQueue.createQueue(QUEUE_NAME);

        // Uploads file into S3 bucket
        S3UploadObject.uploadObject(BUCKET_NAME, path.toString(), s3path, false);

        // Sends two messages msg1=bucketName, msg2=s3FilePath
        System.out.println("[Client] Notifying INBOX queue");
        SQSSendMessage.sendMessages(QUEUE_NAME, BUCKET_NAME + ':' + s3path);
    }
}

