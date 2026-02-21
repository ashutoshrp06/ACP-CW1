package uk.ac.ed.inf.acpTutorial.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DynamoDbConfiguration {

    @Value("${acp.dynamodb:http://localhost:4566}")
    private String dynamoDbEndpoint;

    public String getDynamoDbEndpoint() {
        return dynamoDbEndpoint;
    }
}