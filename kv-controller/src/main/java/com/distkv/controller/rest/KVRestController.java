package com.distkv.controller.rest;

import com.distkv.controller.grpc.KVGrpcClientService;
import com.distkv.controller.registry.WorkerRegistry;
import com.distkv.grpc.GetResponse;
import com.distkv.grpc.PutResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/kv")
@RequiredArgsConstructor
@Slf4j
public class KVRestController {

    private final WorkerRegistry registry;
    private final KVGrpcClientService grpcClient;

    @jakarta.annotation.PostConstruct
    public void init() {
        registry.setGrpcClient(grpcClient);
    }

    @PostMapping("/{key}")
    public ResponseEntity<?> put(@PathVariable("key") String key, @RequestBody String value) {
        log.info("Received PUT request for key: {}", key);
        registry.registerKey(key);
        String[] replicas = registry.getHasher().getReplicas(key, 3);
        log.info("Chosen replicas for key {}: {}", key, java.util.Arrays.toString(replicas));
        
        if (replicas.length < 2) {
            return ResponseEntity.status(503).body("Not enough workers for quorum");
        }

        Map<String, Long> vectorClock = new HashMap<>(); 
        vectorClock.put("v1", System.currentTimeMillis());

        List<String> syncSuccesses = new ArrayList<>();
        String asyncWorker = null;

        // Try primary and secondary synchronously first
        CompletableFuture<Boolean> w1 = CompletableFuture.supplyAsync(() -> {
            try { return grpcClient.put(replicas[0], key, value, vectorClock).getSuccess(); }
            catch (Exception e) { return false; }
        });
        CompletableFuture<Boolean> w2 = CompletableFuture.supplyAsync(() -> {
            try { return grpcClient.put(replicas[1], key, value, vectorClock).getSuccess(); }
            catch (Exception e) { return false; }
        });

        boolean s1 = false;
        boolean s2 = false;
        try { s1 = w1.get(5, java.util.concurrent.TimeUnit.SECONDS); } catch (Exception ignored) {}
        try { s2 = w2.get(5, java.util.concurrent.TimeUnit.SECONDS); } catch (Exception ignored) {}

        if (s1) syncSuccesses.add(replicas[0]);
        if (s2) syncSuccesses.add(replicas[1]);

        // Quorum check and fallback to 3rd replica if needed
        if (syncSuccesses.size() < 2 && replicas.length == 3 && replicas[2] != null) {
            log.info("Primary/Secondary failed to reach quorum, fallback to tertiary: {}", replicas[2]);
            try {
                if (grpcClient.put(replicas[2], key, value, vectorClock).getSuccess()) {
                    syncSuccesses.add(replicas[2]);
                }
            } catch (Exception e) {
                log.error("Tertiary fallback failed: {}", e.getMessage());
            }
        } else if (syncSuccesses.size() >= 2 && replicas.length == 3 && replicas[2] != null) {
            // Already reached quorum, fire the 3rd one asynchronously
            asyncWorker = replicas[2];
            CompletableFuture.runAsync(() -> {
                try { grpcClient.replicate(replicas[2], key, value, vectorClock); }
                catch (Exception e) { log.warn("Async replication failed for {}: {}", replicas[2], e.getMessage()); }
            });
        }

        if (syncSuccesses.size() >= 2) {
            String msg = String.format("Stored successfully. {Synchronous Replicas: %s}, {Asynchronous Replica: %s}", 
                    syncSuccesses, (asyncWorker != null ? asyncWorker : "None (Quorum fallback used)"));
            log.info(msg);
            return ResponseEntity.ok(msg);
        }

        return ResponseEntity.status(500).body("Failed to reach quorum. Successes: " + syncSuccesses.size());
    }

    @GetMapping("/{key}")
    public ResponseEntity<?> get(@PathVariable("key") String key) {
        log.info("Received GET request for key: {}", key);
        String[] replicas = registry.getHasher().getReplicas(key, 3);
        
        List<GetResponse> responses = new ArrayList<>();
        Map<String, GetResponse> replicaResponses = new HashMap<>();

        GetResponse quickResponse = null;
        for (String replica : replicas) {
            if (replica == null) continue;
            try {
                GetResponse res = grpcClient.get(replica, key);
                replicaResponses.put(replica, res);
                if (res.getFound()) {
                    responses.add(res);
                    if (quickResponse == null) quickResponse = res;
                }
            } catch (Exception e) {
                log.warn("Failed to get from replica {}: {}", replica, e.getMessage());
            }
        }

        // Find latest version (Source of Truth)
        GetResponse latest = null;
        String sourceWorker = null;

        for (Map.Entry<String, GetResponse> entry : replicaResponses.entrySet()) {
            if (entry.getValue().getFound()) {
                if (latest == null || isNewer(entry.getValue(), latest)) {
                    latest = entry.getValue();
                    sourceWorker = entry.getKey();
                }
            }
        }

        if (latest == null) {
            return ResponseEntity.notFound().build();
        }

        // Read Repair: Fix stale or missing replicas in background
        GetResponse finalLatest = latest;
        CompletableFuture.runAsync(() -> {
            for (String replica : replicas) {
                if (replica == null) continue;
                GetResponse current = replicaResponses.get(replica);
                if (current == null || !current.getFound() || isNewer(finalLatest, current)) {
                    log.info("Read Repair: Updating stale/missing replica {} for key {}", replica, key);
                    try {
                        grpcClient.put(replica, key, finalLatest.getValue(), finalLatest.getVectorClockMap());
                    } catch (Exception e) {
                        log.warn("Read Repair failed for {}: {}", replica, e.getMessage());
                    }
                }
            }
        });

        String result = String.format("Value: %s (Source: %s)", latest.getValue(), sourceWorker);
        log.info("Key '{}' retrieved from {}: {}", key, sourceWorker, latest.getValue());
        return ResponseEntity.ok(result);
    }

    private boolean isNewer(GetResponse res1, GetResponse res2) {
        // Simple timestamp comparison in "v1" for now, or sum of all clock values
        long v1 = res1.getVectorClockMap().values().stream().mapToLong(L -> L).sum();
        long v2 = res2.getVectorClockMap().values().stream().mapToLong(L -> L).sum();
        return v1 > v2;
    }

    @GetMapping("/worker/{workerId}")
    public ResponseEntity<?> getWorkerData(@PathVariable("workerId") String workerId) {
        log.info("Received request for all data from worker: {}", workerId);
        if (!registry.getActiveWorkers().containsKey(workerId)) {
            return ResponseEntity.status(404).body("Worker not found or offline: " + workerId);
        }

        try {
            Map<String, String> data = grpcClient.getAll(workerId);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            log.error("Failed to retrieve data from worker {}: {}", workerId, e.getMessage());
            return ResponseEntity.status(500).body("Error retrieving data from worker: " + e.getMessage());
        }
    }

    @GetMapping("/workers")
    public ResponseEntity<?> listWorkers() {
        return ResponseEntity.ok(registry.getActiveWorkers());
    }
}
