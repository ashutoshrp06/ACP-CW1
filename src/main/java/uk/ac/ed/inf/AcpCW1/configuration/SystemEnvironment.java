package uk.ac.ed.inf.AcpCW1.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import software.amazon.awssdk.regions.Region;

@Configuration
public class SystemEnvironment {

    @Value( "${acp.ilp-endpoint:https://ilp-rest-2025-all-assigned.azurewebsites.net/}")
    private String ilpServiceEndpoint;

    @Bean(name = "ilpServiceEndpoint")
    public String getIlpServiceEndpoint(){
        return ilpServiceEndpoint;
    }

    public static final String AWS_USER = "test";
    public String getAwsUser() {
        return AWS_USER;
    }

    public static final String AWS_SECRET = "test";
    public String getAwsSecret() {
        return AWS_SECRET;
    }

    public static final Region AWS_REGION = Region.US_EAST_1;
    public Region getAwsRegion() {
        return AWS_REGION;
    }

    @Profile("test")
    @Bean(name = "abc")
    public String getAbc() {
        return "abc";
    }

    @Profile("! test")
    @Bean(name = "abc")
    public String mockAbc() {
        return "mock";
    }

}
