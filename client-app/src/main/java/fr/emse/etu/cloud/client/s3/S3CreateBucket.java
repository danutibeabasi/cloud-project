package fr.emse.etu.cloud.client.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

public class S3CreateBucket {

    /**
     * Creates a new S3 bucket.
     *
     * @param bucketName The name of the bucket to be created.
     * @throws S3Exception If any error occurs during bucket creation.
     */
    public static void createBucket(String bucketName) {
        System.out.printf("[S3] Creating bucket '%s' ...\n", bucketName);

        try (S3Client s3Client = S3Client.builder().build()) {
            S3Waiter s3Waiter = s3Client.waiter();
            CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Client.createBucket(createBucketRequest);
            HeadBucketRequest waitRequest = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Waiter.waitUntilBucketExists(waitRequest);
            System.out.println("[S3] Bucket '" + bucketName + "' is successfully created and ready for use.");
            System.out.println("[S3] Bucket creation process completed.");
            // We add some delay in order to do not have any error because of the time it takes to create the bucket
            Thread.sleep(2000);
        } catch (S3Exception e) {
            System.err.println("[S3] Bucket creation error: " + e.awsErrorDetails().errorMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

}
