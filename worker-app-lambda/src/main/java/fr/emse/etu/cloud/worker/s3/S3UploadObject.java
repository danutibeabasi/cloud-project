package fr.emse.etu.cloud.worker.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;


public class S3UploadObject {

    /**
     * Uploads an object from a specified file path to S3.
     *
     * @param bucketName  The name of the S3 bucket.
     * @param s3objectKey The key of the object in the bucket.
     * @param inputPath   The path to write the object to.
     * @param overwrite   Whether to overwrite existent file in S3 bucket.
     */
    public static void uploadObject(String bucketName, String inputPath, String s3objectKey, boolean overwrite) {
        try (S3Client s3Client = S3Client.builder().build()) {
            ListObjectsRequest listObjects = ListObjectsRequest.builder().bucket(bucketName).build();
            List<S3Object> objects = s3Client.listObjects(listObjects).contents();

            // If file does not already exist
            System.out.println("[S3] Uploading object '" + s3objectKey + "' to bucket '" + bucketName + "'...");
            if (objects.stream().noneMatch((S3Object object) -> object.key().equals(s3objectKey)) || overwrite) {
                String uploadResult = uploadObjectToS3(bucketName, inputPath, s3objectKey);
                System.out.println(uploadResult.isEmpty() ?
                        "[S3] Upload failed" :
                        "[S3] Upload completed - ETag: " + uploadResult);
            } else
                System.out.println("[S3] File already exists");
        }
    }

    /**
     * Uploads an object to an Amazon S3 bucket.
     *
     * @param filePath   The path of the file to upload.
     * @param bucketName The name of the S3 bucket.
     * @param objectKey  The key for the object to upload.
     * @return The ETag of the uploaded object or an empty string if the upload fails.
     */
    private static String uploadObjectToS3(String bucketName, String filePath, String objectKey) {
        try (S3Client s3Client = S3Client.builder().build()) {

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build();

            PutObjectResponse putResponse = s3Client.putObject(putObjectRequest,
                    RequestBody.fromBytes(readFileAsBytes(filePath)));

            return putResponse.eTag();

        } catch (S3Exception e) {
            System.err.println("[S3] Error during object upload: " + e.getMessage());
            System.exit(1);
        }
        return "";
    }

    /**
     * Reads a file from a given path and returns it as a byte array.
     *
     * @param filePath The path of the file to read.
     * @return A byte array of the file's contents or null if an error occurs.
     */
    private static byte[] readFileAsBytes(String filePath) {

        byte[] fileData = null;

        try (FileInputStream fileInputStream = new FileInputStream(filePath)) {
            fileData = new byte[fileInputStream.available()];
            fileInputStream.read(fileData);
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            e.printStackTrace();
        }
        return fileData;
    }
}
