package uk.ac.ed.inf.AcpCW1.controller;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.inf.AcpCW1.dto.SplitterRequest;
import uk.ac.ed.inf.AcpCW1.dto.TransformRequest;
import uk.ac.ed.inf.AcpCW1.service.MessagingService;

import java.util.List;

@RestController
@RequestMapping("/api/v1/acp")
public class MessagingController {

    private final MessagingService messagingService;

    public MessagingController(MessagingService messagingService) {
        this.messagingService = messagingService;
    }

    @PutMapping("/messages/rabbitmq/{queueName}/{messageCount}")
    public ResponseEntity<Void> putRabbitMq(
            @PathVariable String queueName,
            @PathVariable int messageCount) {
        try {
            messagingService.putRabbitMq(queueName, messageCount);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PutMapping("/messages/kafka/{writeTopic}/{messageCount}")
    public ResponseEntity<Void> putKafka(
            @PathVariable String writeTopic,
            @PathVariable int messageCount) {
        try {
            messagingService.putKafka(writeTopic, messageCount);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/messages/rabbitmq/{queueName}/{timeoutInMsec}")
    public ResponseEntity<List<String>> getRabbitMq(
            @PathVariable String queueName,
            @PathVariable long timeoutInMsec) {
        try {
            return ResponseEntity.ok(messagingService.getRabbitMq(queueName, timeoutInMsec));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/messages/kafka/{readTopic}/{timeoutInMsec}")
    public ResponseEntity<List<String>> getKafka(
            @PathVariable String readTopic,
            @PathVariable long timeoutInMsec) {
        try {
            return ResponseEntity.ok(messagingService.getKafka(readTopic, timeoutInMsec));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/messages/sorted/rabbitmq/{queueName}/{messagesToConsider}")
    public ResponseEntity<List<JsonNode>> getSortedRabbitMq(
            @PathVariable String queueName,
            @PathVariable int messagesToConsider) {
        try {
            return ResponseEntity.ok(messagingService.getSortedRabbitMq(queueName, messagesToConsider));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/messages/sorted/kafka/{topic}/{messagesToConsider}")
    public ResponseEntity<List<JsonNode>> getSortedKafka(
            @PathVariable String topic,
            @PathVariable int messagesToConsider) {
        try {
            return ResponseEntity.ok(messagingService.getSortedKafka(topic, messagesToConsider));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/splitter")
    public ResponseEntity<Void> splitter(@RequestBody SplitterRequest request) {
        try {
            messagingService.splitter(request);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/transformMessages")
    public ResponseEntity<Void> transformMessages(@RequestBody TransformRequest request) {
        try {
            messagingService.transformMessages(request);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}