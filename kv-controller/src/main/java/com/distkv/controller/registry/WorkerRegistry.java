package com.distkv.controller.registry;

import com.distkv.common.ConsistentHasher;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class WorkerRegistry {
    private static final int VIRTUAL_NODES = 100;
    private static final long HEARTBEAT_TIMEOUT = 6000; // 6 seconds

    @Getter
    private final ConsistentHasher hasher = new ConsistentHasher(VIRTUAL_NODES);
    private final Map<String, WorkerInfo> activeWorkers = new ConcurrentHashMap<>();
    private final Set<String> allKeys = ConcurrentHashMap.newKeySet();

    // We'll need access to the gRPC client to trigger re-replication
    // Using a setter or lazy injection to avoid circular dependency
    private com.distkv.controller.grpc.KVGrpcClientService grpcClient;

    public void setGrpcClient(com.distkv.controller.grpc.KVGrpcClientService grpcClient) {
        this.grpcClient = grpcClient;
    }

    public void registerKey(String key) {
        allKeys.add(key);
    }

    public void registerHeartbeat(String workerId, String address, int port) {
        if (!activeWorkers.containsKey(workerId)) {
            log.info("New worker registered: {} at {}:{}", workerId, address, port);
            hasher.addWorker(workerId);
        }
        activeWorkers.put(workerId, new WorkerInfo(address, port, System.currentTimeMillis()));
    }

    public Map<String, WorkerInfo> getActiveWorkers() {
        return activeWorkers;
    }

    @Scheduled(fixedRate = 2000)
    public void removeTimedOutWorkers() {
        long now = System.currentTimeMillis();
        activeWorkers.entrySet().removeIf(entry -> {
            boolean timedOut = now - entry.getValue().getLastHeartbeat() > 6000;
            if (timedOut) {
                log.warn("Worker {} timed out and removed", entry.getKey());
                hasher.removeWorker(entry.getKey());
                reReplicateKeysFrom(entry.getKey());
                return true;
            }
            return false;
        });
    }

    private void reReplicateKeysFrom(String failedWorkerId) {
        log.info("Starting proactive re-replication for failed worker: {}", failedWorkerId);
        CompletableFuture.runAsync(() -> {
            for (String key : allKeys) {
                // Check if the failed worker was supposed to have this key
                // Note: getReplicas uses the CURRENT ring, so we can't easily see if it WAS there
                // But we can check if it SHOULD be there now and if it's "new"
                String[] currentReplicas = hasher.getReplicas(key, 3);
                
                // If the failed worker was historically a replica, it's missing now.
                // We don't have historical data easily, but we know the ring shifted.
                // Any key that has a new node in its top 3 needs a sync.
                
                // Let's find one healthy replica to serve as source
                String source = null;
                for (String r : currentReplicas) {
                    if (r != null && !r.equals(failedWorkerId) && activeWorkers.containsKey(r)) {
                        source = r;
                        break;
                    }
                }

                if (source != null) {
                    // Try to update all current replicas from the source
                    for (String target : currentReplicas) {
                        if (target != null && !target.equals(source)) {
                            try {
                                log.info("Checking/Syncing key '{}' to promoted/existing replica: {}", key, target);
                                // Fetch from source and push to target
                                var getRes = grpcClient.get(source, key);
                                if (getRes.getFound()) {
                                    grpcClient.put(target, key, getRes.getValue(), getRes.getVectorClockMap());
                                }
                            } catch (Exception e) {
                                log.warn("Failed to re-replicate key '{}' to {}: {}", key, target, e.getMessage());
                            }
                        }
                    }
                }
            }
            log.info("Background re-replication finished for {}", failedWorkerId);
        });
    }

    @Getter
    public static class WorkerInfo {
        private final String address;
        private final int port;
        private final long lastHeartbeat;

        public WorkerInfo(String address, int port, long lastHeartbeat) {
            this.address = address;
            this.port = port;
            this.lastHeartbeat = lastHeartbeat;
        }
    }
}
