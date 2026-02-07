package uk.ac.ed.inf.acpTutorial.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.S3Object;
import uk.ac.ed.inf.acpTutorial.configuration.PostgresConfiguration;
import uk.ac.ed.inf.acpTutorial.configuration.S3Configuration;
import uk.ac.ed.inf.acpTutorial.configuration.SystemEnvironment;
import uk.ac.ed.inf.acpTutorial.dto.Drone;
import uk.ac.ed.inf.acpTutorial.service.PostgresService;

import java.net.URI;
import java.util.List;

@RestController()
@RequestMapping("/api/v1/acp/postgres")
public class PostgresController {

    private final PostgresConfiguration postgresConfiguration;
    private final PostgresService postgresService;

    public PostgresController(PostgresConfiguration postgresConfiguration, PostgresService postgresService) {
        this.postgresConfiguration = postgresConfiguration;
        this.postgresService = postgresService;
    }
    @GetMapping("/endpoint")
    public String getPostgresEndpoint() {
        return postgresConfiguration.getPostgresEndpoint();
    }

    @GetMapping("/drones")
    public List<Drone> listDrones() {
        return postgresService.getAllDrones();
    }

    @PutMapping("/drones-jdbc")
    public ResponseEntity<String> createDroneUsingJdpc(@RequestBody Drone drone) {
        return ResponseEntity.ok(postgresService.createDroneUsingJdbc(drone));
    }

    @PutMapping("/drones-jpa")
    public ResponseEntity<String> createDroneUsingJpa(@RequestBody Drone drone) {
        return ResponseEntity.ok(postgresService.createDroneUsingJpa(drone));
    }

    @DeleteMapping("/drones/{droneId}")
    public ResponseEntity<Void> deleteDrone(@PathVariable String droneId) {
        postgresService.deleteDrone(droneId);
        return ResponseEntity.ok().build();
    }


}
