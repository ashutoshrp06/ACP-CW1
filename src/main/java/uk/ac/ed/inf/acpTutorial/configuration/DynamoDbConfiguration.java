package uk.ac.ed.inf.acpTutorial.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DynamoDbConfiguration {

    @Value( "${acp.dynamodb-endpoint:http://dynamodb.localhost.localstack.cloud:4566}")
    private String dynamoDbEndpoint;

    @Bean(name = "dynamoDbEndpoint")
    public String getDynamoDbEndpoint(){
        return dynamoDbEndpoint;
    }
}
