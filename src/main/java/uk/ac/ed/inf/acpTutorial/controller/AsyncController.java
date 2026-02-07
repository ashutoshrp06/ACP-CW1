package uk.ac.ed.inf.acpTutorial.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.inf.acpTutorial.configuration.PostgresConfiguration;
import uk.ac.ed.inf.acpTutorial.dto.Drone;
import uk.ac.ed.inf.acpTutorial.service.AsyncService;
import uk.ac.ed.inf.acpTutorial.service.PostgresService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RestController()
@RequestMapping("/api/v1/acp/async-test")
public class AsyncController {

    private final AsyncService asyncService;

    public AsyncController(AsyncService asyncService) {
        this.asyncService = asyncService;
    }
    private final HashMap<String, CompletableFuture<String>> pendingRequestMap = new HashMap<>();

    @GetMapping("/new-correlation-id")
    public ResponseEntity<String> getAsyncEndpoint() {
        var correlationId = UUID.randomUUID().toString();
        pendingRequestMap.put(correlationId, asyncService.asyncMethod());
        return ResponseEntity.ok(correlationId);
    }

    @GetMapping("/by-correlation-id/{correlationId}")
    public ResponseEntity<String> getAsyncStatus(@PathVariable String correlationId) {
        if (!pendingRequestMap.containsKey(correlationId)) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(pendingRequestMap.get(correlationId).join());
    }

    @GetMapping("/sync")
    public ResponseEntity<String> getSync() {
        return ResponseEntity.ok(asyncService.asyncMethod().join());
    }

    @GetMapping("/parallel")
    public ResponseEntity<List<String>> getParallel() {
        List<CompletableFuture<String>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add(asyncService.asyncMethod());
        }

        // Wait for all and collect results - clean with .join()
        return ResponseEntity.ok(futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
    }

    @GetMapping("/parallelInLoop")
    public ResponseEntity<List<String>> getParallelInLoop() {
        List<String> ids = List.of("1", "2", "3", "4", "5");

        List<CompletableFuture<String>> futures =
                ids
                .stream()
                .map(id -> CompletableFuture.supplyAsync(() -> { return id + " : " + asyncService.asyncMethod().join();}))
                .toList(); // Wait for all

        // Wait for all and collect results - clean with .join()
        return ResponseEntity.ok(futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
    }





}
