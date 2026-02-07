package uk.ac.ed.inf.acpTutorial.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;
import uk.ac.ed.inf.acpTutorial.service.DynamoDbService;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class SqsMessageListener {

    @Value("${acp.sqs-table-in-dynamodb:acp-sqs-table}")
    private String sqsTableInDynamoDb;

    private final DynamoDbService dynamoDbService;

    @JmsListener(destination = "${acp.sqs-queue-name:acp-queue}")
    public void receiveMessage(String message) {
        log.info("Received message from SQS: " + message);
        processMessage(message);
    }


    private void processMessage(String message) {
        log.info("Processing message: {}", message);
        dynamoDbService.saveMessageToDynamoDb(sqsTableInDynamoDb, UUID.randomUUID().toString(), message);
    }
}
