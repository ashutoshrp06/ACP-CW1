package uk.ac.ed.inf.acpTutorial.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class S3Configuration {

    @Value( "${acp.s3-endpoint:http://s3.localhost.localstack.cloud:4566}")
    private String s3Endpoint;

    @Value( "${acp.s3-bucket:https://ilp-rest-2025-all-assigned.azurewebsites.net/}")
    private String s3Bucket;

    @Bean(name = "s3Endpoint")
    public String getS3Endpoint(){
        return s3Endpoint;
    }

    @Bean(name = "s3Bucket")
    public String getS3Bucket(){
        return s3Bucket;
    }

}
