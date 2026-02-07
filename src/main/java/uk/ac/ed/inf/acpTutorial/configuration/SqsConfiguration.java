package uk.ac.ed.inf.acpTutorial.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SqsConfiguration {

    @Value( "${acp.sqs-endpoint:http://sqs.localhost.localstack.cloud:4566}")
    private String sqsEndpoint;

    @Bean(name = "sqsEndpoint")
    public String getSqsEndpoint(){
        return sqsEndpoint;
    }
}
