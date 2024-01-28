package fr.emse.etu.cloud.consolidator.sqs;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

public class SQSSendMessage {

    /**
     * This method is used to send a batch of messages to an SQS queue. It includes details about the bucket and the file.
     *
     * @param queueName The name of the SQS queue.
     * @param msg       The name of the S3 bucket.
     * @throws SqsException If any issue occurs while sending messages.
     */
    public static void sendMessages(String queueName, String msg) {
        System.out.println("[SQS] Sending message ...");

        try (SqsClient sqsClient = SqsClient.create()) {
            String queueUrl = sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).queueUrl();
            SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(msg)
                    .build();
            sqsClient.sendMessage(sendMessageRequest);
            System.out.println("[SQS] Message successfully sent.");
        } catch (SqsException e) {
            System.err.println("[SQS] Error encountered: " + e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
