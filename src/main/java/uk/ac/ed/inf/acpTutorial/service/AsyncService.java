package uk.ac.ed.inf.acpTutorial.service;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StopWatch;
import uk.ac.ed.inf.acpTutorial.dto.Drone;
import uk.ac.ed.inf.acpTutorial.entity.DroneEntity;
import uk.ac.ed.inf.acpTutorial.mapper.DroneMapper;
import uk.ac.ed.inf.acpTutorial.repository.DroneRepository;

import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service

// the magic annotation to enable async
@EnableAsync

public class AsyncService {

    @Async
    public CompletableFuture<String> asyncMethod(){
        System.out.println("Async method init at: " + LocalDateTime.now());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            System.err.println("Error sleeping: " + e.getMessage());
        }
        System.out.println("Async method terminated at: " + LocalDateTime.now());
        return CompletableFuture.completedFuture("Done for " + UUID.randomUUID());
    }
}
