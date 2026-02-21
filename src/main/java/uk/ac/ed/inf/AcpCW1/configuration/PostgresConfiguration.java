package uk.ac.ed.inf.AcpCW1.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PostgresConfiguration {

    @Value("${acp.postgres:jdbc:postgresql://localhost:5432/acp}")
    private String postgresEndpoint;

    public String getPostgresEndpoint() {
        return postgresEndpoint;
    }
}