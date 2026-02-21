package uk.ac.ed.inf.acpTutorial.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class S3Configuration {

    // ACP_S3 env var maps to acp.s3 via Spring Boot relaxed binding
    @Value("${acp.s3:http://localhost:4566}")
    private String s3Endpoint;

    public String getS3Endpoint() {
        return s3Endpoint;
    }
}