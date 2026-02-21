package uk.ac.ed.inf.acpTutorial.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import uk.ac.ed.inf.acpTutorial.configuration.DynamoDbConfiguration;
import uk.ac.ed.inf.acpTutorial.configuration.SystemEnvironment;

import java.net.URI;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class DynamoDbService {

    private final DynamoDbConfiguration dynamoDbConfiguration;
    private final SystemEnvironment systemEnvironment;

    public DynamoDbService(DynamoDbConfiguration dynamoDbConfiguration, SystemEnvironment systemEnvironment) {
        this.dynamoDbConfiguration = dynamoDbConfiguration;
        this.systemEnvironment = systemEnvironment;
    }

    /**
     * Lists all table names in DynamoDB.
     */
    public List<String> listTables() {
        return getDynamoDbClient().listTables().tableNames();
    }

    /**
     * Scans all items in a table. Returns each item as a raw attribute map.
     * The caller is responsible for serialization.
     */
    public List<Map<String, AttributeValue>> scanAllItems(String table) {
        return getDynamoDbClient()
                .scan(ScanRequest.builder().tableName(table).build())
                .items();
    }

    /**
     * Gets a single item by its partition (HASH) key value.
     * Returns null if the item does not exist.
     */
    public Map<String, AttributeValue> getItemByKey(String table, String keyName, String keyValue) {
        GetItemResponse response = getDynamoDbClient().getItem(
                GetItemRequest.builder()
                        .tableName(table)
                        .key(Map.of(keyName, AttributeValue.builder().s(keyValue).build()))
                        .build()
        );
        // DynamoDB returns an empty map (not null) when item is not found
        return (response.hasItem() && !response.item().isEmpty()) ? response.item() : null;
    }

    /**
     * Writes a fully-constructed item map to DynamoDB.
     * The caller is responsible for including the partition key in the map.
     */
    public void putItem(String table, Map<String, AttributeValue> item) {
        getDynamoDbClient().putItem(
                PutItemRequest.builder()
                        .tableName(table)
                        .item(item)
                        .build()
        );
    }

    /**
     * Discovers the HASH (partition) key attribute name for a given table.
     */
    public String getTablePrimaryKey(String table) {
        DescribeTableResponse response = getDynamoDbClient().describeTable(
                DescribeTableRequest.builder().tableName(table).build()
        );
        return response.table().keySchema().stream()
                .filter(k -> k.keyType() == KeyType.HASH)
                .map(KeySchemaElement::attributeName)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No HASH key found for table: " + table));
    }

    public DynamoDbClient getDynamoDbClient() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(dynamoDbConfiguration.getDynamoDbEndpoint()))
                .region(systemEnvironment.getAwsRegion())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(systemEnvironment.getAwsUser(), systemEnvironment.getAwsSecret())))
                .build();
    }
}