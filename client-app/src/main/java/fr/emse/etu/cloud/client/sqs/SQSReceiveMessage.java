package fr.emse.etu.cloud.client.sqs;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.List;

public class SQSReceiveMessage {

    /**
     * Receives messages from the specified SQS queue.
     *
     * @param queueName The URL of the SQS queue.
     * @return A list of messages, or null if an error occurs.
     * @throws SqsException Thrown if an error occurs in message retrieval.
     */
    public static List<Message> receiveMessages(String queueName) {
        System.out.println("[SQS] Receiving messages from " + queueName + "...");

        try (SqsClient sqsClient = SqsClient.create()) {
            String queueUrl = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
            ReceiveMessageRequest messageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .build();
            List<Message> receivedMessages = sqsClient.receiveMessage(messageRequest).messages();
            System.out.println("[SQS] Receiving " + receivedMessages.size() + " messages:");
            for (Message msg : receivedMessages)
                System.out.println("\t Msg" + receivedMessages.indexOf(msg) + ": \t" + msg.body());
            return receivedMessages;

        } catch (SqsException e) {
            System.err.println("[SQS] Error encountered: " + e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }
}
