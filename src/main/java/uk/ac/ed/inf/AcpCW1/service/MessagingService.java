package uk.ac.ed.inf.AcpCW1.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import uk.ac.ed.inf.AcpCW1.configuration.KafkaConfiguration;
import uk.ac.ed.inf.AcpCW1.dto.SplitterRequest;
import uk.ac.ed.inf.AcpCW1.dto.TransformRequest;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

@Slf4j
@Service
public class MessagingService {

    private static final String TRANSFORM_VERSIONS_KEY = "transform_state";

    private final RabbitTemplate rabbitTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaConfiguration kafkaConfiguration;

    @Value("${acp.sid:s2821285}")
    private String sid;

    public MessagingService(RabbitTemplate rabbitTemplate,
                            KafkaTemplate<String, String> kafkaTemplate,
                            StringRedisTemplate redisTemplate,
                            ObjectMapper objectMapper,
                            KafkaConfiguration kafkaConfiguration) {
        this.rabbitTemplate = rabbitTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.kafkaConfiguration = kafkaConfiguration;
    }

    // -------------------------------------------------------------------------
    // PUT RabbitMQ
    // -------------------------------------------------------------------------
    public void putRabbitMq(String queueName, int messageCount) throws Exception {
        for (int i = 0; i < messageCount; i++) {
            Map<String, Object> msg = new LinkedHashMap<>();
            msg.put("uid", sid);
            msg.put("counter", i);
            rabbitTemplate.convertAndSend(queueName, objectMapper.writeValueAsString(msg));
        }
    }

    // -------------------------------------------------------------------------
    // PUT Kafka
    // -------------------------------------------------------------------------
    public void putKafka(String writeTopic, int messageCount) throws Exception {
        for (int i = 0; i < messageCount; i++) {
            Map<String, Object> msg = new LinkedHashMap<>();
            msg.put("uid", sid);
            msg.put("counter", i);
            kafkaTemplate.send(writeTopic, objectMapper.writeValueAsString(msg));
        }
        kafkaTemplate.flush();
    }

    // -------------------------------------------------------------------------
    // GET RabbitMQ (read until timeout)
    // -------------------------------------------------------------------------
    public List<String> getRabbitMq(String queueName, long timeoutMs) {
        List<String> messages = new ArrayList<>();
        // Return 50ms before the outer deadline to safely stay within timeoutMs + 200ms window
        long deadline = System.currentTimeMillis() + timeoutMs - 50;
        while (System.currentTimeMillis() < deadline) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) break;
            Message msg = rabbitTemplate.receive(queueName, Math.min(remaining, 50));
            if (msg != null) {
                messages.add(new String(msg.getBody(), StandardCharsets.UTF_8));
            }
        }
        return messages;
    }

    // -------------------------------------------------------------------------
    // GET Kafka (read until timeout)
    // -------------------------------------------------------------------------
    public List<String> getKafka(String readTopic, long timeoutMs) {
        List<String> messages = new ArrayList<>();
        // Return 100ms before outer deadline
        long deadline = System.currentTimeMillis() + timeoutMs - 100;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(buildKafkaConsumerProps())) {
            List<TopicPartition> tps = getTopicPartitions(consumer, readTopic);
            consumer.assign(tps);
            consumer.seekToBeginning(tps);
            while (System.currentTimeMillis() < deadline) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) break;
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Math.min(remaining, 100)));
                for (ConsumerRecord<String, String> r : records) {
                    messages.add(r.value());
                }
            }
        } catch (Exception e) {
            log.error("getKafka error for topic {}", readTopic, e);
        }
        return messages;
    }

    // -------------------------------------------------------------------------
    // GET sorted RabbitMQ (read exactly N, sort by Id ascending)
    // -------------------------------------------------------------------------
    public List<JsonNode> getSortedRabbitMq(String queueName, int messagesToConsider) throws Exception {
        List<JsonNode> messages = new ArrayList<>();
        while (messages.size() < messagesToConsider) {
            Message raw = rabbitTemplate.receive(queueName, 10_000);
            if (raw == null) break;
            messages.add(objectMapper.readTree(new String(raw.getBody(), StandardCharsets.UTF_8)));
        }
        messages.sort(Comparator.comparingInt(n -> n.get("Id").asInt()));
        return messages;
    }

    // -------------------------------------------------------------------------
    // GET sorted Kafka (read exactly N, sort by Id ascending)
    // -------------------------------------------------------------------------
    public List<JsonNode> getSortedKafka(String topic, int messagesToConsider) throws Exception {
        List<JsonNode> messages = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(buildKafkaConsumerProps())) {
            List<TopicPartition> tps = getTopicPartitions(consumer, topic);
            consumer.assign(tps);
            consumer.seekToBeginning(tps);
            while (messages.size() < messagesToConsider) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5_000));
                if (records.isEmpty()) break;
                for (ConsumerRecord<String, String> r : records) {
                    if (messages.size() < messagesToConsider) {
                        messages.add(objectMapper.readTree(r.value()));
                    }
                }
            }
        }
        messages.sort(Comparator.comparingInt(n -> n.get("Id").asInt()));
        return messages;
    }

    // -------------------------------------------------------------------------
    // POST splitter
    // -------------------------------------------------------------------------
    public void splitter(SplitterRequest req) throws Exception {
        // Load existing running state from Redis (method is called multiple times cumulatively)
        long countEven = parseLong(redisTemplate.opsForValue().get("count_even"));
        long countOdd  = parseLong(redisTemplate.opsForValue().get("count_odd"));
        double sumEven = parseDouble(redisTemplate.opsForValue().get("sum_even"));
        double sumOdd  = parseDouble(redisTemplate.opsForValue().get("sum_odd"));

        for (int i = 0; i < req.getMessageCount(); i++) {
            Message raw = rabbitTemplate.receive(req.getReadQueue(), 10_000);
            if (raw == null) continue;

            String body = new String(raw.getBody(), StandardCharsets.UTF_8);
            JsonNode node = objectMapper.readTree(body);
            int id = node.get("Id").asInt();
            double value = node.get("Value").asDouble();

            if (id % 2 == 0) {
                redisTemplate.opsForHash().put(req.getRedisHashEven(), String.valueOf(id), body);
                kafkaTemplate.send(req.getWriteTopicEven(), body);
                countEven++;
                sumEven += value;
                redisTemplate.opsForValue().set("count_even", String.valueOf(countEven));
                redisTemplate.opsForValue().set("sum_even", String.valueOf(sumEven));
                redisTemplate.opsForValue().set("average_even",
                        String.format("%.2f", sumEven / countEven));
            } else {
                redisTemplate.opsForHash().put(req.getRedisHashOdd(), String.valueOf(id), body);
                kafkaTemplate.send(req.getWriteTopicOdd(), body);
                countOdd++;
                sumOdd += value;
                redisTemplate.opsForValue().set("count_odd", String.valueOf(countOdd));
                redisTemplate.opsForValue().set("sum_odd", String.valueOf(sumOdd));
                redisTemplate.opsForValue().set("average_odd",
                        String.format("%.2f", sumOdd / countOdd));
            }
        }

        kafkaTemplate.flush();
    }

    // -------------------------------------------------------------------------
    // POST transformMessages
    // -------------------------------------------------------------------------
    public void transformMessages(TransformRequest req) throws Exception {
        long totalMessagesWritten   = 0;
        long totalMessagesProcessed = 0;
        long tombstoneCount         = 0;
        double totalValueWritten    = 0.0;
        double totalAdded           = 0.0;

        for (int i = 0; i < req.getMessageCount(); i++) {
            Message raw = rabbitTemplate.receive(req.getReadQueue(), 30_000);
            if (raw == null) continue;

            String body = new String(raw.getBody(), StandardCharsets.UTF_8);
            JsonNode node = objectMapper.readTree(body);
            String key = node.get("key").asText();

            if ("TOMBSTONE".equals(key)) {
                tombstoneCount++;
                redisTemplate.delete(TRANSFORM_VERSIONS_KEY);

                // Tombstone summary counts as 1 message written
                totalMessagesWritten++;

                Map<String, Object> summary = new LinkedHashMap<>();
                summary.put("totalMessagesWritten",   totalMessagesWritten);
                summary.put("totalMessagesProcessed", totalMessagesProcessed);
                summary.put("totalRedisUpdates",      totalMessagesProcessed + tombstoneCount);
                summary.put("totalValueWritten",      totalValueWritten);
                summary.put("totalAdded",             totalAdded);
                rabbitTemplate.convertAndSend(req.getWriteQueue(),
                        objectMapper.writeValueAsString(summary));

            } else {
                int version  = node.get("version").asInt();
                double value = node.get("value").asDouble();

                Object storedVersion = redisTemplate.opsForHash()
                        .get(TRANSFORM_VERSIONS_KEY, key);
                int redisVersion = (storedVersion == null) ? -1
                        : Integer.parseInt(storedVersion.toString());

                String outBody;
                double outValue;

                if (redisVersion < version) {
                    // New or newer version: update Redis, add 10.5
                    redisTemplate.opsForHash().put(TRANSFORM_VERSIONS_KEY, key,
                            String.valueOf(version));
                    outValue = value + 10.5;
                    totalMessagesProcessed++;
                    totalAdded += 10.5;

                    ObjectNode out = objectMapper.createObjectNode();
                    out.put("key", key);
                    out.put("version", version);
                    out.put("value", outValue);
                    outBody = objectMapper.writeValueAsString(out);
                } else {
                    // Same or older version: pass through 1:1
                    outValue = value;
                    outBody = body;
                }

                totalValueWritten += outValue;
                totalMessagesWritten++;
                rabbitTemplate.convertAndSend(req.getWriteQueue(), outBody);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    private Properties buildKafkaConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfiguration.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "acp-consumer-" + UUID.randomUUID());
        return props;
    }

    private List<TopicPartition> getTopicPartitions(KafkaConsumer<String, String> consumer,
                                                    String topic) {
        List<PartitionInfo> infos = consumer.partitionsFor(topic);
        if (infos == null) return List.of();
        return infos.stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .toList();
    }

    private long parseLong(String value) {
        if (value == null || value.isBlank()) return 0L;
        try { return Long.parseLong(value); } catch (NumberFormatException e) { return 0L; }
    }

    private double parseDouble(String value) {
        if (value == null || value.isBlank()) return 0.0;
        try { return Double.parseDouble(value); } catch (NumberFormatException e) { return 0.0; }
    }
}