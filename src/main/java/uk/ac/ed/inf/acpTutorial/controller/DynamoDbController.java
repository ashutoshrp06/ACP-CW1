package uk.ac.ed.inf.acpTutorial.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import uk.ac.ed.inf.acpTutorial.configuration.DynamoDbConfiguration;
import uk.ac.ed.inf.acpTutorial.configuration.SystemEnvironment;

import java.net.URI;
import java.util.List;

@RestController()
@RequestMapping("/api/v1/acp/dynamo")
@RequiredArgsConstructor
public class DynamoDbController {

    private final DynamoDbConfiguration dynamoDbConfiguration;
    private final SystemEnvironment systemEnvironment;


    @GetMapping("/endpoint")
    public String getDynamoDbEndpoint () {
        return dynamoDbConfiguration.getDynamoDbEndpoint();
    }

    @GetMapping("/tables")
    public List<String> listTables() {
        return getDynamoDbClient().listTables().tableNames();
    }

    @GetMapping("/list-objects/{table}")
    public List<String> listTableObjects(@PathVariable String table) {
        return getDynamoDbClient()
                .scanPaginator(b -> b.tableName(table))
                .stream()
                .flatMap(r -> r.items().stream())
                .flatMap(i -> i.keySet().stream())
                .distinct()
                .toList();
    }

    @PutMapping("/create-table/{table}")
    public void createTable(@PathVariable String table) {
        getDynamoDbClient().createTable(b -> b.tableName(table)
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("key")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .keySchema(KeySchemaElement.builder()
                        .attributeName("key")
                        .keyType(KeyType.HASH)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(5L)
                        .writeCapacityUnits(5L)
                        .build())
        );
    }

    @PutMapping("/create-object/{table}/{key}")
    public void createObject(@PathVariable String table, @PathVariable String key, @RequestBody String objectContent) {
        getDynamoDbClient().putItem(b -> b.tableName(table).item(
                java.util.Map.of("key", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(key).build(),
                        "content", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(objectContent).build())
        ));
    }

    private DynamoDbClient getDynamoDbClient() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(dynamoDbConfiguration.getDynamoDbEndpoint()))
                .region(systemEnvironment.getAwsRegion())
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(systemEnvironment.getAwsUser(), systemEnvironment.getAwsSecret())))
                .build();
    }
}
