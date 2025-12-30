package com.distkv.controller.grpc;

import com.distkv.controller.registry.WorkerRegistry;
import com.distkv.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class KVGrpcClientService {

    private final WorkerRegistry registry;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, KVServiceGrpc.KVServiceBlockingStub> stubs = new ConcurrentHashMap<>();

    private KVServiceGrpc.KVServiceBlockingStub getStub(String workerId) {
        WorkerRegistry.WorkerInfo info = registry.getActiveWorkers().get(workerId);
        if (info == null) {
            log.warn("No worker info found for ID: {}", workerId);
            return null;
        }

        return stubs.computeIfAbsent(workerId, id -> {
            log.info("Creating gRPC channel for worker {} at {}:{}", id, info.getAddress(), info.getPort());
            ManagedChannel channel = ManagedChannelBuilder.forAddress(info.getAddress(), info.getPort())
                    .usePlaintext()
                    .build();
            channels.put(id, channel);
            return KVServiceGrpc.newBlockingStub(channel);
        });
    }

    public PutResponse put(String workerId, String key, String value, Map<String, Long> vectorClock) {
        KVServiceGrpc.KVServiceBlockingStub stub = getStub(workerId);
        if (stub == null) return PutResponse.newBuilder().setSuccess(false).setMessage("Worker offline").build();
        
        try {
            return stub.withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS).put(PutRequest.newBuilder()
                    .setKey(key)
                    .setValue(value)
                    .putAllVectorClock(vectorClock)
                    .build());
        } catch (Exception e) {
            log.error("gRPC PUT failed for worker {}: {}", workerId, e.getMessage());
            throw e;
        }
    }

    public GetResponse get(String workerId, String key) {
        KVServiceGrpc.KVServiceBlockingStub stub = getStub(workerId);
        if (stub == null) return GetResponse.newBuilder().setFound(false).build();
        
        try {
            return stub.withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS).get(GetRequest.newBuilder().setKey(key).build());
        } catch (Exception e) {
            log.error("gRPC GET failed for worker {}: {}", workerId, e.getMessage());
            throw e;
        }
    }

    public ReplicateResponse replicate(String workerId, String key, String value, Map<String, Long> vectorClock) {
        KVServiceGrpc.KVServiceBlockingStub stub = getStub(workerId);
        if (stub == null) return ReplicateResponse.newBuilder().setSuccess(false).build();
        
        try {
            return stub.withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS).replicate(ReplicateRequest.newBuilder()
                    .setKey(key)
                    .setValue(value)
                    .putAllVectorClock(vectorClock)
                    .build());
        } catch (Exception e) {
            log.error("gRPC REPLICATE failed for worker {}: {}", workerId, e.getMessage());
            throw e;
        }
    }

    public Map<String, String> getAll(String workerId) {
        KVServiceGrpc.KVServiceBlockingStub stub = getStub(workerId);
        if (stub == null) return null;

        Map<String, String> results = new HashMap<>();
        try {
            stub.withDeadlineAfter(10, java.util.concurrent.TimeUnit.SECONDS)
                .sync(SyncRequest.newBuilder().setWorkerId("controller").build())
                .forEachRemaining(entry -> results.put(entry.getKey(), entry.getValue()));
            return results;
        } catch (Exception e) {
            log.error("gRPC SYNC/GetAll failed for worker {}: {}", workerId, e.getMessage());
            throw e;
        }
    }
}
