package uk.ac.ed.inf.acpTutorial.controller;

import io.swagger.v3.oas.annotations.Parameter;
import jdk.jfr.ContentType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.services.dynamodb.model.*;
import uk.ac.ed.inf.acpTutorial.configuration.DynamoDbConfiguration;
import uk.ac.ed.inf.acpTutorial.service.DynamoDbService;
import java.util.List;

@RestController()
@RequestMapping("/api/v1/acp/dynamo")
public class DynamoDbController {

    private final DynamoDbConfiguration dynamoDbConfiguration;
    private final DynamoDbService dynamoDbService;

    public DynamoDbController(DynamoDbConfiguration dynamoDbConfiguration, DynamoDbService dynamoDbService) {
        this.dynamoDbConfiguration = dynamoDbConfiguration;
        this.dynamoDbService = dynamoDbService;
    }

    @GetMapping("/endpoint")
    public String getDynamoDbEndpoint () {
        return dynamoDbConfiguration.getDynamoDbEndpoint();
    }

    @GetMapping("/tables")
    public List<String> listTables() {
        return dynamoDbService.listTables();
    }

    @GetMapping(path = "/list-objects/{table}", produces = "application/json")
    public ResponseEntity<String> listTableObjects(@PathVariable String table) {
        return ResponseEntity.ok("[" + String.join(", " , dynamoDbService.listTableObjects(table)) + "]");
    }

    @PutMapping("/create-table/{table}")
    public void createTable(@PathVariable String table) {
        dynamoDbService.createTable(table);
    }

    @PutMapping("/create-object/{table}/{key}")
    public void createObject(@PathVariable String table, @PathVariable String key, @RequestBody String objectContent) {
        dynamoDbService.createObject(table, key, objectContent);
    }

    @GetMapping("/primary-key/{table}")
    public String getTablePrimaryKey(
            @Parameter(name = "table", description = "The name of the DynamoDB table")
            @PathVariable(required = true)
            String table) {

        return dynamoDbService.getTablePrimaryKey(table);
    }
}
