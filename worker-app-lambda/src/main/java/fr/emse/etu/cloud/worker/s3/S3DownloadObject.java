package fr.emse.etu.cloud.worker.s3;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class S3DownloadObject {

    /**
     * Downloads an object from S3 and writes it to a specified file path.
     *
     * @param bucketName The name of the S3 bucket.
     * @param objectKey  The key of the object in the bucket.
     * @param outputPath The path to write the object to.
     */
    public static void downloadObject(String bucketName, String objectKey, String outputPath) {
        try (S3Client s3Client = S3Client.builder().build()) {
            ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(bucketName).build();
            List<S3Object> objects = s3Client.listObjects(listObjects).contents();

            // If file exists
            if (objects.stream().anyMatch((S3Object x) -> x.key().equals(objectKey))) {
                System.out.println("[S3] Downloading object from Amazon S3 and saving to the local disk...");
                GetObjectRequest request = GetObjectRequest.builder().key(objectKey).bucket(bucketName).build();

                ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(request);
                File outputFile = new File(outputPath);
                Files.createDirectories(Path.of(outputFile.getParent()));
                s3Client.getObject(request, Paths.get(outputPath));
//                byte[] fileData = objectBytes.asByteArray();
//                try (OutputStream outputStream = Files.newOutputStream(outputFile.toPath())) {
//                    outputStream.write(fileData);
//                    System.out.println("[S3] File downloaded into " + outputFile.toPath().toUri());
//                }
            } else {
                System.err.println("[S3] File not found in S3 Bucket: " + objectKey);
            }
        } catch (IOException e) {
            System.err.println("[S3] Error writing file: " + e.getMessage());
            e.printStackTrace();
        } catch (S3Exception e) {
            System.err.println("[S3] S3 error: " + e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
