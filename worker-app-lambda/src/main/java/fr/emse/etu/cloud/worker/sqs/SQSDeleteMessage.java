package fr.emse.etu.cloud.worker.sqs;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.List;

public class SQSDeleteMessage {

    /**
     * Deletes a batch of messages from a specified SQS queue.
     *
     * @param queueName    The name of the SQS queue from which messages are to be deleted.
     * @param messageBatch A list of messages that are to be deleted.
     * @throws SqsException If any error occurs during the deletion process.
     */
    public static void deleteMessages(String queueName, List<Message> messageBatch) {
        System.out.println("[SQS] Deleting messages from " + queueName + "...");

        try (SqsClient sqsClient = SqsClient.create()) {
            String queueUrl = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();

            for (Message msg : messageBatch) {
                DeleteMessageRequest messageDeletionRequest = DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(msg.receiptHandle()).build();
                sqsClient.deleteMessage(messageDeletionRequest);
            }
            System.out.println("[SQS] Deleted " + messageBatch.size() + " messages successfully");

        } catch (SqsException e) {
            System.err.println("[SQS] Error during message deletion: " + e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

}
