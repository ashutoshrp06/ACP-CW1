package uk.ac.ed.inf.AcpCW1.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.inf.AcpCW1.dto.DroneApiResponse;
import uk.ac.ed.inf.AcpCW1.dto.ProcessRequest;
import uk.ac.ed.inf.AcpCW1.service.AcpService;

import java.util.List;

@RestController
@RequestMapping("/api/v1/acp")
public class AcpMainController {

    private final AcpService acpService;

    public AcpMainController(AcpService acpService) {
        this.acpService = acpService;
    }

    // S3 GET endpoints

    @GetMapping("/all/s3/{bucket}")
    public ResponseEntity<ArrayNode> getAllS3Objects(@PathVariable String bucket) {
        try {
            return ResponseEntity.ok(acpService.getAllS3Objects(bucket));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/single/s3/{bucket}/{key}")
    public ResponseEntity<JsonNode> getSingleS3Object(
            @PathVariable String bucket,
            @PathVariable String key) {
        try {
            JsonNode result = acpService.getSingleS3Object(bucket, key);
            if (result == null) return ResponseEntity.notFound().build();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    // DynamoDB GET endpoints

    @GetMapping("/all/dynamo/{table}")
    public ResponseEntity<ArrayNode> getAllDynamoItems(@PathVariable String table) {
        try {
            return ResponseEntity.ok(acpService.getAllDynamoItems(table));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/single/dynamo/{table}/{key}")
    public ResponseEntity<JsonNode> getSingleDynamoItem(
            @PathVariable String table,
            @PathVariable String key) {
        try {
            JsonNode result = acpService.getSingleDynamoItem(table, key);
            if (result == null) return ResponseEntity.notFound().build();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    // Postgres GET endpoint

    @GetMapping("/all/postgres/{table}")
    public ResponseEntity<ArrayNode> getAllPostgresRows(@PathVariable String table) {
        try {
            return ResponseEntity.ok(acpService.getAllPostgresRows(table));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    // POST endpoints

    @PostMapping("/process/dump")
    public ResponseEntity<List<DroneApiResponse>> processDump(@RequestBody ProcessRequest request) {
        try {
            return ResponseEntity.ok(acpService.fetchAndEnrichDrones(request.getUrlPath()));
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/process/dynamo")
    public ResponseEntity<Void> processDynamo(@RequestBody ProcessRequest request) {
        try {
            acpService.processDynamo(request.getUrlPath());
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/process/s3")
    public ResponseEntity<Void> processS3(@RequestBody ProcessRequest request) {
        try {
            acpService.processS3(request.getUrlPath());
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/process/postgres/{table}")
    public ResponseEntity<Void> processPostgres(
            @PathVariable String table,
            @RequestBody ProcessRequest request) {
        try {
            acpService.processPostgres(request.getUrlPath(), table);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/copy-content/dynamo/{table}")
    public ResponseEntity<Void> copyContentDynamo(@PathVariable String table) {
        try {
            acpService.copyContentDynamo(table);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }

    @PostMapping("/copy-content/S3/{table}")
    public ResponseEntity<Void> copyContentS3(@PathVariable String table) {
        try {
            acpService.copyContentS3(table);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
    }
}