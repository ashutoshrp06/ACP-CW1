package uk.ac.ed.inf.acpTutorial.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class DynamoDbControllerTest {

    @Autowired
    private DynamoDbController dynamoDbController;

    @Test
    public void testCreateTableSucceeds() {
        String tableName = "test-table-" + System.currentTimeMillis();
        dynamoDbController.createTable(tableName);
        
        // Verify table exists
        assertTrue(dynamoDbController.listTables().contains(tableName));
    }


    @Test
    public void testCreateObjectSucceeds() {
        String tableName = "test-table-" + System.currentTimeMillis();
        dynamoDbController.createTable(tableName);
        
        String key = "key-1";
        String content = "hello world";
        dynamoDbController.createObject(tableName, key, content);
        
        // Verify object (key) exists in list
        assertTrue(Objects.requireNonNull(dynamoDbController.listTableObjects(tableName).getBody()).contains("key"));
    }
}
