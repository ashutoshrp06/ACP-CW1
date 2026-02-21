package uk.ac.ed.inf.AcpCW1.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import uk.ac.ed.inf.AcpCW1.configuration.S3Configuration;
import uk.ac.ed.inf.AcpCW1.configuration.SystemEnvironment;
import uk.ac.ed.inf.AcpCW1.dto.Capability;
import uk.ac.ed.inf.AcpCW1.dto.DroneApiResponse;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class AcpService {

    private final DynamoDbService dynamoDbService;
    private final S3Configuration s3Configuration;
    private final SystemEnvironment systemEnvironment;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Value("${acp.sid}")
    private String sid;

    public AcpService(DynamoDbService dynamoDbService,
                      S3Configuration s3Configuration,
                      SystemEnvironment systemEnvironment,
                      JdbcTemplate jdbcTemplate,
                      ObjectMapper objectMapper) {
        this.dynamoDbService = dynamoDbService;
        this.s3Configuration = s3Configuration;
        this.systemEnvironment = systemEnvironment;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    // S3

    public ArrayNode getAllS3Objects(String bucket) {
        ArrayNode result = objectMapper.createArrayNode();
        S3Client s3 = getS3Client();
        List<S3Object> objects = s3.listObjectsV2(
                ListObjectsV2Request.builder().bucket(bucket).build()
        ).contents();
        for (S3Object obj : objects) {
            byte[] bytes = s3.getObjectAsBytes(
                    GetObjectRequest.builder().bucket(bucket).key(obj.key()).build()
            ).asByteArray();
            String content = new String(bytes, StandardCharsets.UTF_8);
            try {
                result.add(objectMapper.readTree(content));
            } catch (Exception e) {
                // Non-JSON content: wrap as a JSON string value
                result.add(content);
            }
        }
        return result;
    }

    public JsonNode getSingleS3Object(String bucket, String key) {
        try {
            byte[] bytes = getS3Client().getObjectAsBytes(
                    GetObjectRequest.builder().bucket(bucket).key(key).build()
            ).asByteArray();
            String content = new String(bytes, StandardCharsets.UTF_8);
            return objectMapper.readTree(content);
        } catch (NoSuchKeyException e) {
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    // DynamoDB

    public ArrayNode getAllDynamoItems(String table) {
        ArrayNode result = objectMapper.createArrayNode();
        List<Map<String, AttributeValue>> items = dynamoDbService.scanAllItems(table);
        for (Map<String, AttributeValue> item : items) {
            result.add(attributeMapToJson(item));
        }
        return result;
    }

    public JsonNode getSingleDynamoItem(String table, String key) {
        String keyName = dynamoDbService.getTablePrimaryKey(table);
        Map<String, AttributeValue> item = dynamoDbService.getItemByKey(table, keyName, key);
        if (item == null) return null;
        return attributeMapToJson(item);
    }

    // Postgres

    public ArrayNode getAllPostgresRows(String table) {
        ArrayNode result = objectMapper.createArrayNode();
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                "SELECT * FROM " + sid + "." + table
        );
        for (Map<String, Object> row : rows) {
            result.add(objectMapper.valueToTree(row));
        }
        return result;
    }

    // Process endpoints

    public List<DroneApiResponse> fetchAndEnrichDrones(String urlPath) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(urlPath))
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        DroneApiResponse[] drones = objectMapper.readValue(response.body(), DroneApiResponse[].class);
        for (DroneApiResponse drone : drones) {
            drone.setCostPer100Moves(computeCostPer100Moves(drone));
        }
        return List.of(drones);
    }

    public void processDynamo(String urlPath) throws Exception {
        List<DroneApiResponse> drones = fetchAndEnrichDrones(urlPath);
        String keyName = dynamoDbService.getTablePrimaryKey(sid);
        for (DroneApiResponse drone : drones) {
            String json = objectMapper.writeValueAsString(drone);
            Map<String, AttributeValue> item = new HashMap<>();
            item.put(keyName, AttributeValue.builder().s(drone.getName()).build());
            item.put("data", AttributeValue.builder().s(json).build());
            dynamoDbService.putItem(sid, item);
        }
    }

    public void processS3(String urlPath) throws Exception {
        List<DroneApiResponse> drones = fetchAndEnrichDrones(urlPath);
        S3Client s3 = getS3Client();
        for (DroneApiResponse drone : drones) {
            String json = objectMapper.writeValueAsString(drone);
            s3.putObject(
                    PutObjectRequest.builder().bucket(sid).key(drone.getName()).build(),
                    RequestBody.fromString(json)
            );
        }
    }

    public void processPostgres(String urlPath, String table) throws Exception {
        List<DroneApiResponse> drones = fetchAndEnrichDrones(urlPath);
        String sql = "INSERT INTO " + sid + "." + table
                + " (id, name, costPer100Moves, cooling, heating, capacity, maxMoves, costPerMove, costInitial, costFinal)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                + " ON CONFLICT (name) DO NOTHING";
        for (DroneApiResponse drone : drones) {
            Capability cap = drone.getCapability();
            boolean cooling   = cap != null && cap.isCooling();
            boolean heating   = cap != null && cap.isHeating();
            double  capacity  = cap != null ? cap.getCapacity()  : 0.0;
            int     maxMoves  = cap != null ? cap.getMaxMoves()  : 0;
            double  perMove   = cap != null ? cap.getCostPerMove()  : 0.0;
            double  initial   = cap != null ? cap.getCostInitial()  : 0.0;
            double  finalCost = cap != null ? cap.getCostFinal()    : 0.0;
            jdbcTemplate.update(sql,
                    drone.getId(),
                    drone.getName(),
                    drone.getCostPer100Moves(),
                    cooling,
                    heating,
                    capacity,
                    maxMoves,
                    perMove,
                    initial,
                    finalCost
            );
        }
    }

    // Copy-content endpoints

    public void copyContentDynamo(String table) {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                "SELECT * FROM " + sid + "." + table
        );
        String keyName = dynamoDbService.getTablePrimaryKey(sid);
        for (Map<String, Object> row : rows) {
            try {
                String json = objectMapper.writeValueAsString(row);
                String uuid = UUID.randomUUID().toString();
                Map<String, AttributeValue> item = new HashMap<>();
                item.put(keyName, AttributeValue.builder().s(uuid).build());
                item.put("data", AttributeValue.builder().s(json).build());
                dynamoDbService.putItem(sid, item);
            } catch (Exception e) {
                throw new RuntimeException("Failed to write row to DynamoDB", e);
            }
        }
    }

    public void copyContentS3(String table) {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                "SELECT * FROM " + sid + "." + table
        );
        S3Client s3 = getS3Client();
        for (Map<String, Object> row : rows) {
            try {
                String json = objectMapper.writeValueAsString(row);
                String uuid = UUID.randomUUID().toString();
                s3.putObject(
                        PutObjectRequest.builder().bucket(sid).key(uuid).build(),
                        RequestBody.fromString(json)
                );
            } catch (Exception e) {
                throw new RuntimeException("Failed to write row to S3", e);
            }
        }
    }

    // Helpers

    private double computeCostPer100Moves(DroneApiResponse drone) {
        Capability cap = drone.getCapability();
        double initial  = cap != null ? cap.getCostInitial()  : 0.0;
        double finalCost = cap != null ? cap.getCostFinal()   : 0.0;
        double perMove  = cap != null ? cap.getCostPerMove()  : 0.0;
        double raw = initial + finalCost + (perMove * 100);
        return BigDecimal.valueOf(raw).setScale(2, RoundingMode.HALF_UP).doubleValue();
    }

    private ObjectNode attributeMapToJson(Map<String, AttributeValue> item) {
        ObjectNode node = objectMapper.createObjectNode();
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            String attrName = entry.getKey();
            AttributeValue av = entry.getValue();
            if (av.s() != null) {
                node.put(attrName, av.s());
            } else if (av.n() != null) {
                node.put(attrName, new BigDecimal(av.n()));
            } else if (av.bool() != null) {
                node.put(attrName, av.bool());
            } else if (Boolean.TRUE.equals(av.nul())) {
                node.putNull(attrName);
            } else if (av.hasL()) {
                ArrayNode arr = objectMapper.createArrayNode();
                for (AttributeValue el : av.l()) {
                    arr.add(attributeValueToJson(el));
                }
                node.set(attrName, arr);
            } else if (av.hasM()) {
                node.set(attrName, attributeMapToJson(av.m()));
            } else {
                node.putNull(attrName);
            }
        }
        return node;
    }

    private JsonNode attributeValueToJson(AttributeValue av) {
        if (av.s() != null) return objectMapper.valueToTree(av.s());
        if (av.n() != null) return objectMapper.valueToTree(new BigDecimal(av.n()));
        if (av.bool() != null) return objectMapper.valueToTree(av.bool());
        if (Boolean.TRUE.equals(av.nul())) return objectMapper.nullNode();
        if (av.hasL()) {
            ArrayNode arr = objectMapper.createArrayNode();
            for (AttributeValue el : av.l()) arr.add(attributeValueToJson(el));
            return arr;
        }
        if (av.hasM()) return attributeMapToJson(av.m());
        return objectMapper.nullNode();
    }

    private S3Client getS3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create(s3Configuration.getS3Endpoint()))
                .forcePathStyle(true)
                .region(systemEnvironment.getAwsRegion())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                systemEnvironment.getAwsUser(),
                                systemEnvironment.getAwsSecret())))
                .build();
    }
}