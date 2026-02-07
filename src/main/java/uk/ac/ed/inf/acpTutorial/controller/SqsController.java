package uk.ac.ed.inf.acpTutorial.controller;

import org.springframework.jms.core.JmsTemplate;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.services.sqs.model.Message;
import uk.ac.ed.inf.acpTutorial.service.SqsService;

import jakarta.jms.JMSException;
import jakarta.jms.TextMessage;
import java.util.List;

@RestController
@RequestMapping("/api/v1/acp/sqs")
public class SqsController {

    private final SqsService sqsService;
    private final JmsTemplate jmsTemplate;

    public SqsController(SqsService sqsService, JmsTemplate jmsTemplate) {
        this.sqsService = sqsService;
        this.jmsTemplate = jmsTemplate;
    }

    @GetMapping("/queues")
    public List<String> listQueues() {
        return sqsService.listQueues();
    }

    @PutMapping("/create-queue/{queueName}")
    public String createQueue(@PathVariable String queueName) {
        return sqsService.createQueue(queueName);
    }

    @DeleteMapping("/delete-queue")
    public void deleteQueue(@RequestParam String queueUrl) {
        sqsService.deleteQueue(queueUrl);
    }

    @PostMapping("/send-message/{queueUrl}")
    public void sendMessage(@PathVariable String queueUrl, @RequestBody String messageBody) {
        sqsService.sendMessage(queueUrl, messageBody);
    }

    @GetMapping("/receive-messages/{queueUrl}")
    public List<String> receiveMessages(@PathVariable String queueUrl) {
        return sqsService.receiveMessages(queueUrl).stream().map(Message::body).toList();
    }

    @PostMapping("/jms/send-message")
    public void sendMessageJms(@RequestParam String queueName, @RequestBody String messageBody) {
        jmsTemplate.convertAndSend(queueName, messageBody);
    }

    @GetMapping("/jms/receive-message")
    public String receiveMessageJms(@RequestParam String queueName) throws JMSException {
        Object message = jmsTemplate.receiveAndConvert(queueName);
        if (message instanceof TextMessage) {
            return ((TextMessage) message).getText();
        }
        return message != null ? message.toString() : null;
    }

}
