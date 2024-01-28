package fr.emse.etu.cloud.client.sqs;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class SQSCheckQueue {

    /**
     * Checks if a queue exists
     * @param queueName The name of the SQS queue to check
     * @return a boolean whether it exists
     */
    public static boolean exists(String queueName) {
        try (SqsClient sqsClient = SqsClient.create()) {
            sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean hasMessages(String queueName) {
        try (SqsClient sqsClient = SqsClient.create()) {
            String queueUrl = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();

            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            // Check if the list of messages is empty
            return sqsClient.receiveMessage(receiveMessageRequest).hasMessages();
        }
    }
}
