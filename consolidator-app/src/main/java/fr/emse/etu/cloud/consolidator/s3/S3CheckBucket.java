package fr.emse.etu.cloud.consolidator.s3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3CheckBucket {

    /**
     * Checks if a bucket exists.
     * @param bucketName the bucket name you want to exists
     * @return Whether the bucket exists or not
     */
    public static boolean exists(String bucketName) {
        try (S3Client s3Client = S3Client.builder().build()) {
            return s3Client.listBuckets()
                    .buckets()
                    .stream()
                    .anyMatch(bucket -> bucket.name().equals(bucketName));
        } catch (S3Exception e) {
            System.err.println("[S3] Error checking bucket existence: " + e.awsErrorDetails().errorMessage());
        }
        return false;
    }
}

